from pyspark.sql import SparkSession
import happybase
from analysis_results import AnalysisResults

# Define constants
HBASE_TABLE_NAME = "web_logs"
HBASE_HOST = "localhost"
HBASE_PORT = 9090
CF_ANALYSIS_RESULTS = "analysis_results"

# Create a connection to HBase
connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)

# Define functions for HBase operations
def put_row_to_hbase(row_key, analysis_results):
    """
    Store the analysis results for a single row to HBase.
    """
    table = connection.table(HBASE_TABLE_NAME)
    table.put(row_key.encode(), {
        f"{CF_ANALYSIS_RESULTS}:num_requests".encode(): str(analysis_results.num_requests).encode(),
        f"{CF_ANALYSIS_RESULTS}:num_successful_requests".encode(): str(analysis_results.num_successful_requests).encode(),
        f"{CF_ANALYSIS_RESULTS}:num_failed_requests".encode(): str(analysis_results.num_failed_requests).encode(),
        f"{CF_ANALYSIS_RESULTS}:http_codes".encode(): str(analysis_results.http_codes).encode(),
        f"{CF_ANALYSIS_RESULTS}:http_methods".encode(): str(analysis_results.http_methods).encode(),
        f"{CF_ANALYSIS_RESULTS}:ip_address_access_count".encode(): str(analysis_results.ip_address_access_count).encode(),
        f"{CF_ANALYSIS_RESULTS}:http_method_success_count".encode(): str(analysis_results.http_method_success_count).encode(),
        f"{CF_ANALYSIS_RESULTS}:http_method_failure_count".encode(): str(analysis_results.http_method_failure_count).encode(),
        f"{CF_ANALYSIS_RESULTS}:ip_address_success_count".encode(): str(analysis_results.ip_address_success_count).encode(),
        f"{CF_ANALYSIS_RESULTS}:ip_address_failure_count".encode(): str(analysis_results.ip_address_failure_count).encode(),
        f"{CF_ANALYSIS_RESULTS}:total_response_time".encode(): str(analysis_results.total_response_time).encode(),
        f"{CF_ANALYSIS_RESULTS}:num_successful_requests_with_response_time".encode(): str(analysis_results.num_successful_requests_with_response_time).encode()
    })

def parse_request_line(request_line):
    """
    Parse a single line from the log file and extract the relevant fields.
    """
    split_line = request_line.split()
    ip_address = split_line[0]
    method = split_line[5].replace('"', '')
    url = split_line[6]
    http_version = split_line[7].replace('"', '')
    status_code = int(split_line[8])
    response_size = int(split_line[9])
    return (ip_address, {
        "method": method,
        "url": url,
        "http_version": http_version,
        "status_code": status_code,
        "response_size": response_size
    })

def analyze_logs(logs):
    """
    Analyze the logs and return the results.
    """
    analysis_results = AnalysisResults()

    # Count the number of accesses from each IP address
    ip_address_access_count = logs.map(lambda line: parse_request_line(line)) \
        .map(lambda line: (line[0], 1)) \
        .reduceByKey(lambda a, b: a + b).collectAsMap()
    for ip_address, count in ip_address_access_count.items():
        analysis_results.update_ip_address_access_count(ip_address, count)

    # Count the number of successful and failed requests for each IP address
    ip_address_success_count = logs.filter(lambda line: int(parse_request_line(line)[1]["status_code"] / 100) == 2) \
    .map(lambda line: (parse_request_line(line)[0], 1)) \
    .reduceByKey(lambda a, b: a + b).collectAsMap()
    ip_address_failure_count = logs.filter(lambda line: int(parse_request_line(line)[1]["status_code"] / 100) != 2) \
        .map(lambda line: (parse_request_line(line)[0], 1)) \
        .reduceByKey(lambda a, b: a + b).collectAsMap()
    for ip_address, count in ip_address_success_count.items():
        analysis_results.update_ip_address_success_count(ip_address, count)
    for ip_address, count in ip_address_failure_count.items():
        analysis_results.update_ip_address_failure_count(ip_address, count)

    # Count the number of successful and failed requests for each HTTP method
    http_method_success_count = logs.filter(lambda line: int(parse_request_line(line)[1]["status_code"] / 100) == 2) \
        .map(lambda line: (parse_request_line(line)[1]["method"], 1)) \
        .reduceByKey(lambda a, b: a + b).collectAsMap()
    http_method_failure_count = logs.filter(lambda line: int(parse_request_line(line)[1]["status_code"] / 100) != 2) \
        .map(lambda line: (parse_request_line(line)[1]["method"], 1)) \
        .reduceByKey(lambda a, b: a + b).collectAsMap()
    for method, count in http_method_success_count.items():
        analysis_results.update_http_method_success_count(method, count)
    for method, count in http_method_failure_count.items():
        analysis_results.update_http_method_failure_count(method, count)

    # Count the number of successful requests with a response time greater than 1 second
    num_successful_requests_with_response_time = logs.filter(lambda line: int(parse_request_line(line)[1]["status_code"] / 100) == 2 and parse_request_line(line)[1]["response_size"] > 0) \
        .map(lambda line: (parse_request_line(line)[0], parse_request_line(line)[1]["response_size"])) \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: x[1] > 1000) \
        .count()
    analysis_results.update_num_successful_requests_with_response_time(num_successful_requests_with_response_time)

    # Compute the total response time for all successful requests
    total_response_time = logs.filter(lambda line: int(parse_request_line(line)[1]["status_code"] / 100) == 2 and parse_request_line(line)[1]["response_size"] > 0) \
        .map(lambda line: parse_request_line(line)[1]["response_size"]) \
        .reduce(lambda a, b: a + b, 0)
    analysis_results.update_total_response_time(total_response_time)

    # Count the number of requests, successful requests, failed requests, HTTP codes, and HTTP methods
    analysis_results.update_num_requests(logs.count())
    analysis_results.update_num_successful_requests(logs.filter(lambda line: int(parse_request_line(line)[1]["status_code"] / 100) == 2).count())
    analysis_results.update_num_failed_requests(logs.filter(lambda line: int(parse_request_line(line)[1]["status_code"] / 100) != 2).count())
    analysis_results.update_http_codes(logs.map(lambda line: parse_request_line(line)[1]["status_code"]).distinct().collect())
    analysis_results.update_http_methods(logs.map(lambda line: parse_request_line(line)[1]["method"]).distinct().collect())

    # Calculate the average response time for successful requests
    avg_response_time = logs.filter(lambda line: int(parse_request_line(line)[1]["status_code"] / 100) == 2 and parse_request_line(line)[1]["response_size"] > 0).map(lambda line: parse_request_line(line)[1]["response_time"]).reduce(lambda a, b: a + b, 0) / logs.filter(lambda line: int(parse_request_line(line)[1]["status_code"] / 100) == 2 and parse_request_line(line)[1]["response_size"] > 0).count()
    analysis_results.update_avg_response_time(avg_response_time)

    # Calculate the percentage of successful requests
    percentage_successful_requests = logs.filter(lambda line: int(parse_request_line(line)[1]["status_code"] / 100) == 2).count() / logs.count() * 100
    analysis_results.update_percentage_successful_requests(percentage_successful_requests)

    # Calculate the percentage of failed requests
    percentage_failed_requests = logs.filter(lambda line: int(parse_request_line(line)[1]["status_code"] / 100) != 2).count() / logs.count() * 100
    analysis_results.update_percentage_failed_requests(percentage_failed_requests)

    return analysis_results

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

    logs = spark.sparkContext.textFile("hdfs:///user/logs/access.log").collect()

    analysis_results = analyze_logs(logs)
    row_key = None
    
    put_row_to_hbase(row_key, analysis_results)

    spark.stop()