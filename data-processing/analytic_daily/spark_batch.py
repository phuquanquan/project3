import yaml
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import happybase
from parse_log_line import parseApacheLogLine
from analysis_results import AnalysisResults


class SparkBatch:
    def __init__(self, cfg):
        # HDFS config
        self.HDFS_PATH = f"hdfs://{cfg['hdfs']['host']}:{cfg['hdfs']['port']}" + cfg['hdfs']['path']

        # Hbase config
        hbase_config = cfg['hbase']
        self.HBASE_TABLE_NAME = hbase_config['table_name']
        self.HBASE_HOST = hbase_config['host']
        self.HBASE_PORT = hbase_config['port']
        self.CF_ANALYSIS_RESULTS = hbase_config['cf_analysis_results']

        # Spark config
        spark_config = cfg['spark']
        self.SPARK_APP_NAME = spark_config['app_name']
        self.SPARK_MASTER = spark_config['master']
        self.SPARK_LOG_LEVEL = spark_config['log_level']

        # Create a connection to HBase
        self.connection = happybase.Connection(self.HBASE_HOST, port=self.HBASE_PORT)
        self.analysis_results = AnalysisResults()

        # Parse logs
        self.parsed_logs = None
        self.access_logs = None
        self.failed_logs = None

    def create_spark_context(self):
        """
        Create a new SparkContext with the given app name, master URL, log level, and batch interval.
        """
        conf = SparkConf().setAppName(self.SPARK_APP_NAME).setMaster(self.SPARK_MASTER)
        sc = SparkContext.getOrCreate(conf=conf)
        sc.setLogLevel(self.SPARK_LOG_LEVEL)
        return SparkSession(sc)
    
    def put_row_to_hbase(self, row_key):
        """
        Store the analysis results for a single row to HBase.
        """
        table = self.connection.table(self.HBASE_TABLE_NAME)
        table.put(row_key.encode(), {
            f"{self.CF_ANALYSIS_RESULTS}:num_requests".encode(): str(self.analysis_results.num_requests).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:num_successful_requests".encode(): str(self.analysis_results.num_successful_requests).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:num_failed_requests".encode(): str(self.analysis_results.num_failed_requests).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:content_sizes_avg".encode(): str(self.analysis_results.content_sizes_avg).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:content_size_min".encode(): str(self.analysis_results.content_size_min).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:content_size_max".encode(): str(self.analysis_results.content_size_max).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:unique_host_count".encode(): str(self.analysis_results.unique_host_count).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:bad_records".encode(): str(self.analysis_results.bad_records).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:http_codes".encode(): str(self.analysis_results.http_codes).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:endpoints".encode(): str(self.analysis_results.endpoints).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:top_ten_rrr_URLs".encode(): str(self.analysis_results.top_ten_rrr_URLs).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:daily_hosts_list".encode(): str(self.analysis_results.daily_hosts_list).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:avg_daily_req_per_host_list".encode(): str(self.analysis_results.avg_daily_req_per_host_list).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:bad_unique_endpoints_pick_40".encode(): str(self.analysis_results.bad_unique_endpoints_pick_40).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:bad_endpoints_top_20".encode(): str(self.analysis_results.bad_endpoints_top_20).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:err_hosts_top_25".encode(): str(self.analysis_results.err_hosts_top_25).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:err_by_date".encode(): str(self.analysis_results.err_by_date).encode(),
            f"{self.CF_ANALYSIS_RESULTS}:err_hour_list".encode(): str(self.analysis_results.err_hour_list).encode()
        })

    def parseLogs(self):
        sc = self.create_spark_context()
        """ Read and parse log file """
        self.parsed_logs = (sc
                    .textFile(self.HDFS_PATH)
                    .map(parseApacheLogLine)
                    .cache())

        self.access_logs = (self.parsed_logs
                    .filter(lambda s: s[1] == 1)
                    .map(lambda s: s[0])
                    .cache())

        self.failed_logs = (self.parsed_logs
                    .filter(lambda s: s[1] == 0)
                    .map(lambda s: s[0]))
        
        self.analysis_results.update_num_requests(self.parsed_logs.count())
        self.analysis_results.update_num_successful_requests(self.access_logs.count())
        self.analysis_results.update_num_failed_requests(self.failed_logs.count())

    def analyze_sample(self):
        """ Calculate statistics based on the content size."""
        content_sizes = self.access_logs.map(lambda log: log.content_size).cache()
        self.analysis_results.update_content_sizes_avg(content_sizes.reduce(lambda a, b : a + b) / content_sizes.count())
        self.analysis_results.update_content_sizes_min(content_sizes.min())
        self.analysis_results.update_content_sizes_max(content_sizes.max())


        """ Response Code to Count """
        responseCodeToCount = (self.access_logs
                            .map(lambda log: (log.response_code, 1))
                            .reduceByKey(lambda a, b : a + b)
                            .cache())
        self.analysis_results.update_http_code(responseCodeToCount.take(100)) 

        """ Any hosts that has accessed the server more than 10 times.""" 
        hostCountPairTuple = self.access_logs.map(lambda log: (log.host, 1))

        hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b)

        hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)
        self.analysis_results.update_host_more_than_10(hostMoreThan10) 

        """ Visualizing Endpoints"""
        endpoints = (self.access_logs
                .map(lambda log: (log.endpoint, 1))
                .reduceByKey(lambda a, b : a + b)
                .cache())
        self.analysis_results.update_endpoints(endpoints) 

    def analyze_logs(self):
        """" Top Ten Error Endpoints """
        not200 = (self.access_logs
            .filter(lambda log: log.response_code != 200))

        endpointCountPairTuple = (not200
                            .map(lambda log: (log.endpoint, 1)))

        endpointSum = (endpointCountPairTuple
                    .reduceByKey(lambda a, b : a + b))

        topTenErrURLs = (endpointSum
                        .takeOrdered(10, lambda s: -1 * s[1]))
        self.analysis_results.update_top_ten_rrr_URLs(topTenErrURLs) 

        """Number of Unique Hosts"""
        hosts = (self.access_logs
            .map(lambda log: log.host))

        uniqueHosts = (hosts
                    .distinct())

        uniqueHostCount = (uniqueHosts
                        .count())
        self.analysis_results.update_unique_host_count(uniqueHostCount) 

        """Number of Unique Daily Hosts"""
        dayToHostPairTuple = (self.access_logs
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
        self.analysis_results.update_daily_hosts(dailyHostsList) 

        """Average Number of Daily Requests per Hosts"""
        dayAndHostTuple = (self.access_logs
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
        
        self.analysis_results.update_avg_daily_req_per_host_list(avgDailyReqPerHostList) 

    def exploring_404_res(self):
        """Counting 404 Response Codes"""
        badRecords = (self.access_logs
                .filter(lambda log: log.response_code == 404)
                .cache())
        
        self.analysis_results.update_bad_records(badRecords.count()) 
        
        """Listing 404 Response Code Records"""
        badEndpoints = (badRecords
                    .map(lambda log: log.endpoint))

        badUniqueEndpoints = (badEndpoints
                            .distinct())

        badUniqueEndpointsPick40 = (badUniqueEndpoints
                                    .take(40))
        
        self.analysis_results.update_bad_unique_endpoints_pick_40(badUniqueEndpointsPick40) 
        
        
        """Listing the Top Twenty 404 Response Code Endpoints"""
        badEndpointsCountPairTuple = (badRecords
                                .map(lambda log: (log.endpoint, 1)))

        badEndpointsSum = (badEndpointsCountPairTuple
                        .reduceByKey(lambda a, b : a + b))

        badEndpointsTop20 = (badEndpointsSum
                            .takeOrdered(20, lambda s: -1 * s[1]))
        
        self.analysis_results.update_bad_endpoints_top_20(badEndpointsTop20) 
        
        """Listing the Top Twenty-five 404 Response Code Hosts"""
        errHostsCountPairTuple = (badRecords
                            .map(lambda log: (log.host, 1)))

        errHostsSum = (errHostsCountPairTuple
                    .reduceByKey(lambda a, b : a + b))

        errHostsTop25 = (errHostsSum
                        .takeOrdered(25, lambda s: -1 * s[1]))
        
        self.analysis_results.update_err_hosts_top_25(errHostsTop25) 
        
        """Listing 404 Response Codes per Day"""
        errDateCountPairTuple = (badRecords
                            .map(lambda log: (log.date_time.day, 1)))

        errDateSum = (errDateCountPairTuple
                    .reduceByKey(lambda a, b : a + b))

        errDateSorted = (errDateSum
                        .sortByKey()
                        .cache())

        errByDate = (errDateSorted.take(100))

        self.analysis_results.update_err_by_date(errByDate) 

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
        
        self.analysis_results.update_err_hour_list(errHourList) 

    def run(self):
        self.parseLogs()
        self.analyze_sample()
        self.analyze_logs()
        self.exploring_404_res()
        self.put_row_to_hbase(None)


if __name__ == "__main__":
    with open('../config/config.yml', 'r') as f:
        cfg =  yaml.safe_load(f)

    sb = SparkBatch(cfg)
    sb.run()