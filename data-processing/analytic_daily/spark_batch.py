import yaml
from pyspark import SparkContext, SparkConf
import happybase
from parse_log_line import parseApacheLogLine
from analysis_results import AnalysisResults

with open('../config/config.yml', 'r') as f:
    cfg =  yaml.safe_load(f)

# Hbase config
HDFS_PATH = f"hdfs://{cfg['hdfs']['host']}:{cfg['hdfs']['port']}" + cfg['hdfs']['path']

# Hbase_config
hbase_config = cfg['hbase']
HBASE_TABLE_NAME = hbase_config['table_name']
HBASE_HOST = hbase_config['host']
HBASE_PORT = hbase_config['port']
CF_ANALYSIS_RESULTS = hbase_config['cf_analysis_results']

# Spark config
spark_config = cfg['spark']
SPARK_APP_NAME = spark_config['app_name']
SPARK_MASTER = spark_config['master']
SPARK_LOG_LEVEL = spark_config['log_level']
SPARK_BATCH_INTERVAL = spark_config['batch_interval']

# Create a connection to HBase
connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)


def create_spark_context(app_name=SPARK_APP_NAME, master=SPARK_MASTER, log_level=SPARK_LOG_LEVEL, batch_interval=SPARK_BATCH_INTERVAL):
    """
    Create a new SparkContext with the given app name, master URL, log level, and batch interval.
    """
    conf = SparkConf().setAppName(app_name).setMaster(master)
    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel(log_level)
    spark = SparkSession(sc)
    spark.conf.set("spark.sql.streaming.pollingInterval", batch_interval)
    return spark


def put_row_to_hbase(row_key, analysis_results):
    """
    Store the analysis results for a single row to HBase.
    """
    table = connection.table(HBASE_TABLE_NAME)
    table.put(row_key.encode(), {
        f"{CF_ANALYSIS_RESULTS}:num_requests".encode(): str(analysis_results.num_requests).encode(),
        f"{CF_ANALYSIS_RESULTS}:num_successful_requests".encode(): str(analysis_results.num_successful_requests).encode(),
        f"{CF_ANALYSIS_RESULTS}:num_failed_requests".encode(): str(analysis_results.num_failed_requests).encode(),
        f"{CF_ANALYSIS_RESULTS}:content_sizes_avg".encode(): str(analysis_results.content_sizes_avg).encode(),
        f"{CF_ANALYSIS_RESULTS}:content_size_min".encode(): str(analysis_results.content_size_min).encode(),
        f"{CF_ANALYSIS_RESULTS}:content_size_max".encode(): str(analysis_results.content_size_max).encode(),
        f"{CF_ANALYSIS_RESULTS}:unique_host_count".encode(): str(analysis_results.unique_host_count).encode(),
        f"{CF_ANALYSIS_RESULTS}:bad_records".encode(): str(analysis_results.bad_records).encode(),
        f"{CF_ANALYSIS_RESULTS}:http_codes".encode(): str(analysis_results.http_codes).encode(),
        f"{CF_ANALYSIS_RESULTS}:endpoints".encode(): str(analysis_results.endpoints).encode(),
        f"{CF_ANALYSIS_RESULTS}:top_ten_rrr_URLs".encode(): str(analysis_results.top_ten_rrr_URLs).encode(),
        f"{CF_ANALYSIS_RESULTS}:daily_hosts_list".encode(): str(analysis_results.daily_hosts_list).encode(),
        f"{CF_ANALYSIS_RESULTS}:avg_daily_req_per_host_list".encode(): str(analysis_results.avg_daily_req_per_host_list).encode(),
        f"{CF_ANALYSIS_RESULTS}:bad_unique_endpoints_pick_40".encode(): str(analysis_results.bad_unique_endpoints_pick_40).encode(),
        f"{CF_ANALYSIS_RESULTS}:bad_endpoints_top_20".encode(): str(analysis_results.bad_endpoints_top_20).encode(),
        f"{CF_ANALYSIS_RESULTS}:err_hosts_top_25".encode(): str(analysis_results.err_hosts_top_25).encode(),
        f"{CF_ANALYSIS_RESULTS}:err_by_date".encode(): str(analysis_results.err_by_date).encode(),
        f"{CF_ANALYSIS_RESULTS}:err_hour_list".encode(): str(analysis_results.err_hour_list).encode()
    })

def parseLogs(sc, hdfs_path, analysis_results):
    """ Read and parse log file """
    parsed_logs = (sc
                   .textFile(hdfs_path)
                   .map(parseApacheLogLine)
                   .cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    
    analysis_results.update_num_requests(parsed_logs.count())
    analysis_results.update_num_successful_requests(access_logs.count())
    analysis_results.update_num_failed_requests(failed_logs.count())

    return parsed_logs, access_logs, failed_logs

def analyze_sample(access_logs, analysis_results):
    """ Calculate statistics based on the content size."""
    content_sizes = access_logs.map(lambda log: log.content_size).cache()
    analysis_results.update_content_sizes_avg(content_sizes.reduce(lambda a, b : a + b) / content_sizes.count())
    analysis_results.update_content_sizes_min(content_sizes.min())
    analysis_results.update_content_sizes_max(content_sizes.max())


    """ Response Code to Count """
    responseCodeToCount = (access_logs
                        .map(lambda log: (log.response_code, 1))
                        .reduceByKey(lambda a, b : a + b)
                        .cache())
    analysis_results.update_http_code(responseCodeToCount.take(100)) 

    """ Any hosts that has accessed the server more than 10 times.""" 
    hostCountPairTuple = access_logs.map(lambda log: (log.host, 1))

    hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b)

    hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)
    analysis_results.update_host_more_than_10(hostMoreThan10) 

    """ Visualizing Endpoints"""
    endpoints = (access_logs
             .map(lambda log: (log.endpoint, 1))
             .reduceByKey(lambda a, b : a + b)
             .cache())
    analysis_results.update_endpoints(endpoints) 

def analyze_logs(access_logs, analysis_results):
    """" Top Ten Error Endpoints """
    not200 = (access_logs
        .filter(lambda log: log.response_code != 200))

    endpointCountPairTuple = (not200
                        .map(lambda log: (log.endpoint, 1)))

    endpointSum = (endpointCountPairTuple
                .reduceByKey(lambda a, b : a + b))

    topTenErrURLs = (endpointSum
                    .takeOrdered(10, lambda s: -1 * s[1]))
    analysis_results.update_top_ten_rrr_URLs(topTenErrURLs) 

    """Number of Unique Hosts"""
    hosts = (access_logs
         .map(lambda log: log.host))

    uniqueHosts = (hosts
                .distinct())

    uniqueHostCount = (uniqueHosts
                    .count())
    analysis_results.update_unique_host_count(uniqueHostCount) 

    """Number of Unique Daily Hosts"""
    dayToHostPairTuple = (access_logs
                        .map(lambda log: (log.date_time.day, log.host)))

    dayGroupedHosts = (dayToHostPairTuple
                    .groupByKey())

    dayHostCount = (dayGroupedHosts
                .map(lambda s: (s[0], len(set(s[1])))))

    dailyHosts = (dayHostCount
                .sortByKey()
                .cache())
    dailyHostsList = (dailyHosts
                  .take(30))
    analysis_results.update_daily_hosts(dailyHostsList) 

    """Average Number of Daily Requests per Hosts"""
    dayAndHostTuple = (access_logs
                   .map(lambda log: (log.date_time.day, log.host)))

    groupedByDay = (dayAndHostTuple
                    .groupByKey())

    sortedByDay = (groupedByDay
                .sortByKey())

    avgDailyReqPerHost = (sortedByDay
                        .map(lambda (d, h): (d, len(h)/len(set(h))))
                        .cache())

    avgDailyReqPerHostList = (avgDailyReqPerHost
                            .take(30))
    
    analysis_results.update_avg_daily_req_per_host_list(avgDailyReqPerHostList) 

def exploring_404_res(access_logs, analysis_results):
    """Counting 404 Response Codes"""
    badRecords = (access_logs
              .filter(lambda log: log.response_code == 404)
              .cache())
    
    analysis_results.update_bad_records(badRecords.count()) 
    
    """Listing 404 Response Code Records"""
    badEndpoints = (badRecords
                .map(lambda log: log.endpoint))

    badUniqueEndpoints = (badEndpoints
                        .distinct())

    badUniqueEndpointsPick40 = (badUniqueEndpoints
                                .take(40))
    
    analysis_results.update_bad_unique_endpoints_pick_40(badUniqueEndpointsPick40) 
    
    
    """Listing the Top Twenty 404 Response Code Endpoints"""
    badEndpointsCountPairTuple = (badRecords
                              .map(lambda log: (log.endpoint, 1)))

    badEndpointsSum = (badEndpointsCountPairTuple
                    .reduceByKey(lambda a, b : a + b))

    badEndpointsTop20 = (badEndpointsSum
                        .takeOrdered(20, lambda s: -1 * s[1]))
    
    analysis_results.update_bad_endpoints_top_20(badEndpointsTop20) 
    
    """Listing the Top Twenty-five 404 Response Code Hosts"""
    errHostsCountPairTuple = (badRecords
                          .map(lambda log: (log.host, 1)))

    errHostsSum = (errHostsCountPairTuple
                .reduceByKey(lambda a, b : a + b))

    errHostsTop25 = (errHostsSum
                    .takeOrdered(25, lambda s: -1 * s[1]))
    
    analysis_results.update_err_hosts_top_25(errHostsTop25) 
    
    """Listing 404 Response Codes per Day"""
    errDateCountPairTuple = (badRecords
                         .map(lambda log: (log.date_time.day, 1)))

    errDateSum = (errDateCountPairTuple
                .reduceByKey(lambda a, b : a + b))

    errDateSorted = (errDateSum
                    .sortByKey()
                    .cache())

    errByDate = (errDateSorted.take(100))

    analysis_results.update_err_by_date(errByDate) 

    """Hourly 404 Response Codes"""
    hourCountPairTuple = (badRecords
                      .map(lambda log: (log.date_time.hour, 1)))

    hourRecordsSum = (hourCountPairTuple
                    .reduceByKey(lambda a, b : a + b))

    hourRecordsSorted = (hourRecordsSum
                        .sortByKey()
                        .cache())

    errHourList = (hourRecordsSorted
                .take(24))
    
    analysis_results.update_err_hour_list(errHourList) 
    

if __name__ == "__main__":
    """
    Analyze the logs and return the results.
    """
    analysis_results = AnalysisResults()

    spark = create_spark_context()
    
    parsed_logs, access_logs, failed_logs = parseLogs(spark, HDFS_PATH, analysis_results)

    analyze_sample(access_logs, analysis_results)
    analyze_logs(access_logs, analysis_results)
    exploring_404_res(access_logs, analysis_results)

    # Process data by partition and write results to HBase
    put_row_to_hbase(None, analysis_results)

    spark.stop()