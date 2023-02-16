# hadoop fs -put /path/in/linux /hdfs/path
from configuration_rdd import parseLogs
from test_helper import Test
from res_graphing import pie_pct_format
from pyspark.sql import SparkSession, Row
import matplotlib.pyplot as plt

# Get RDD
parsed_logs, access_logs, failed_logs = parseLogs()

## TEST Data cleaning (1c)
# Test.assertEquals(failed_logs.count(), 864, 'incorrect failed_logs.count()')
# Test.assertEquals(parsed_logs.count(), 1891715 , 'incorrect parsed_logs.count()')
# Test.assertEquals(access_logs.count(), parsed_logs.count(), 'incorrect access_logs.count()')

# Calculate statistics based on the content size.
# content_sizes = access_logs.map(lambda log: log.content_size).cache()
# print('Content Size Avg: %i, Min: %i, Max: %s' % (
#     content_sizes.reduce(lambda a, b : a + b) / content_sizes.count(),
#     content_sizes.min(),
#     content_sizes.max()))

# Response Code to Count
responseCodeToCount = (access_logs
                       .map(lambda log: (log.response_code, 1))
                       .reduceByKey(lambda a, b : a + b)
                       .cache())
# responseCodeToCountList = responseCodeToCount.take(100)
# print('Found %d response codes' % len(responseCodeToCountList))
# print('Response Code Counts: %s' % responseCodeToCountList)
# assert len(responseCodeToCountList) == 7
# assert sorted(responseCodeToCountList) == [(200, 940847), (302, 16244), (304, 79824), (403, 58), (404, 6185), (500, 2), (501, 17)]

# (2c) Example: Response Code Graphing with matplotlib
labels = responseCodeToCount.map(lambda x: x[0]).collect()
print(labels)
count = access_logs.count()
fracs = responseCodeToCount.map(lambda x: (float(x[1]) / count)).collect()
print(fracs)

fig = plt.figure(figsize=(4.5, 4.5), facecolor='white', edgecolor='white')
colors = ['yellowgreen', 'lightskyblue', 'gold', 'purple', 'lightcoral', 'yellow', 'black']
explode = (0.05, 0.05, 0.1, 0, 0, 0, 0)
patches, texts, autotexts = plt.pie(fracs, labels=labels, colors=colors,
                                    explode=explode, autopct=pie_pct_format,
                                    shadow=False,  startangle=125)
#plt.hist() <- make histogram
#plt.plot()

for text, autotext in zip(texts, autotexts):
    if autotext.get_text() == '':
        text.set_text('')  # If the slice is small to fit, don't show a text label
plt.legend(labels, loc=(0.80, -0.1), shadow=True)
plt.savefig("pie_chart.png")


# Create a DataFrame and visualize using display()
spark = SparkSession.builder.appName("project3").getOrCreate()

responseCodeToCountRow = responseCodeToCount.map(lambda x: Row(response_code=x[0], count=x[1]))
responseCodeToCountDF = spark.createDataFrame(responseCodeToCountRow)
responseCodeToCountDF.write.csv("responseCodeToCountDF.csv", header=True)

# Any hosts that has accessed the server more than 10 times.
hostCountPairTuple = access_logs.map(lambda log: (log.host, 1))

hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b)

hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)

hostsPick20 = (hostMoreThan10
               .map(lambda s: s[0])
               .take(20))

print ('Any 20 hosts that have accessed more then 10 times: %s' % hostsPick20)

# (2e) Example: Visualizing Endpoints
endpoints = (access_logs
             .map(lambda log: (log.endpoint, 1))
             .reduceByKey(lambda a, b : a + b)
             .cache())
ends = endpoints.map(lambda x: x[0]).collect()
counts = endpoints.map(lambda x: y[1]).collect()

fig = plt.figure(figsize=(8,4.2), facecolor='white', edgecolor='white')
plt.axis([0, len(ends), 0, max(counts)])
plt.grid(b=True, which='major', axis='y')
plt.xlabel('Endpoints')
plt.ylabel('Number of Hits')
plt.plot(counts)
plt.savefig("2e.png") 

# Create an RDD with Row objects
spark = SparkSession.builder.appName("project3").getOrCreate()

endpoint_counts_rdd = endpoints.map(lambda s: Row(endpoint = s[0], num_hits = s[1]))
endpoint_counts_schema_rdd = spark.createDataFrame(endpoint_counts_rdd)

# Display a plot of the distribution of the number of hits across the endpoints.
responseCodeToCountDF.write.csv("responseCodeToCountDF.csv", header=True)

# (2f) Example: Top Endpoints
# Top Endpoints
endpointCounts = (access_logs
                  .map(lambda log: (log.endpoint, 1))
                  .reduceByKey(lambda a, b : a + b))

topEndpoints = endpointCounts.takeOrdered(10, lambda s: -1 * s[1])

print('Top Ten Endpoints: %s' % topEndpoints)
assert topEndpoints == [(u'/images/NASA-logosmall.gif', 59737), (u'/images/KSC-logosmall.gif', 50452), (u'/images/MOSAIC-logosmall.gif', 43890), (u'/images/USA-logosmall.gif', 43664), (u'/images/WORLD-logosmall.gif', 43277), (u'/images/ksclogo-medium.gif', 41336), (u'/ksc.html', 28582), (u'/history/apollo/images/apollo-logo1.gif', 26778), (u'/images/launch-logo.gif', 24755), (u'/', 20292)], 'incorrect Top Ten Endpoints'