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