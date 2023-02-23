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
plt.savefig("./result/2c.png")

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
counts = endpoints.map(lambda x: x[1]).collect()

fig = plt.figure(figsize=(8,4.2), facecolor='white', edgecolor='white')
plt.axis([0, len(ends), 0, max(counts)])
plt.grid(color='grey', which='major', axis='y')
plt.xlabel('Endpoints')
plt.ylabel('Number of Hits')
plt.plot(counts)
plt.savefig("./result/2e.png") 

# # (2f) Example: Top Endpoints
# # Top Endpoints
# endpointCounts = (access_logs
#                   .map(lambda log: (log.endpoint, 1))
#                   .reduceByKey(lambda a, b : a + b))

# topEndpoints = endpointCounts.takeOrdered(10, lambda s: -1 * s[1])

# print('Top Ten Endpoints: %s' % topEndpoints)
# assert topEndpoints == [(u'/images/NASA-logosmall.gif', 59737), (u'/images/KSC-logosmall.gif', 50452), (u'/images/MOSAIC-logosmall.gif', 43890), (u'/images/USA-logosmall.gif', 43664), (u'/images/WORLD-logosmall.gif', 43277), (u'/images/ksclogo-medium.gif', 41336), (u'/ksc.html', 28582), (u'/history/apollo/images/apollo-logo1.gif', 26778), (u'/images/launch-logo.gif', 24755), (u'/', 20292)], 'incorrect Top Ten Endpoints'

# (3a) Exercise: Top Ten Error Endpoints
not200 = (access_logs
          .filter(lambda log: log.response_code != 200))

endpointCountPairTuple = (not200
                     .map(lambda log: (log.endpoint, 1)))

endpointSum = (endpointCountPairTuple
               .reduceByKey(lambda a, b : a + b))

topTenErrURLs = (endpointSum
                 .takeOrdered(10, lambda s: -1 * s[1]))
print('Top Ten failed URLs: %s' % topTenErrURLs)

# TEST Top ten error endpoints (3a)
Test.assertEquals(endpointSum.count(), 7689, 'incorrect count for endpointSum')
Test.assertEquals(topTenErrURLs, [(u'/images/NASA-logosmall.gif', 8761), (u'/images/KSC-logosmall.gif', 7236), (u'/images/MOSAIC-logosmall.gif', 5197), (u'/images/USA-logosmall.gif', 5157), (u'/images/WORLD-logosmall.gif', 5020), (u'/images/ksclogo-medium.gif', 4728), (u'/history/apollo/images/apollo-logo1.gif', 2907), (u'/images/launch-logo.gif', 2811), (u'/', 2199), (u'/images/ksclogosmall.gif', 1622)], 'incorrect Top Ten failed URLs (topTenErrURLs)')

# (3b) Exercise: Number of Unique Hosts
hosts = (access_logs
         .map(lambda log: log.host))

uniqueHosts = (hosts
               .distinct())

uniqueHostCount = (uniqueHosts
                   .count())
print('Unique hosts: %d' % uniqueHostCount)

# TEST Number of unique hosts (3b)
Test.assertEquals(uniqueHostCount, 54507, 'incorrect uniqueHostCount')

# (3c) Exercise: Number of Unique Daily Hosts
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
print('Unique hosts per day: %s' % dailyHostsList)

# TEST Number of unique daily hosts (3c)
Test.assertEquals(dailyHosts.count(), 21, 'incorrect dailyHosts.count()')
Test.assertEquals(dailyHostsList, [(1, 2582), (3, 3222), (4, 4190), (5, 2502), (6, 2537), (7, 4106), (8, 4406), (9, 4317), (10, 4523), (11, 4346), (12, 2864), (13, 2650), (14, 4454), (15, 4214), (16, 4340), (17, 4385), (18, 4168), (19, 2550), (20, 2560), (21, 4134), (22, 4456)], 'incorrect dailyHostsList')
Test.assertTrue(dailyHosts.is_cached, 'incorrect dailyHosts.is_cached')

# (3d) Exercise: Visualizing the Number of Unique Daily Hosts
daysWithHosts = dailyHosts.map(lambda x: x[0]).collect()
hosts = dailyHosts.map(lambda x: x[1]).collect()

# TEST Visualizing unique daily hosts (3d)
test_days = list(range(1, 23))
test_days.remove(2)
Test.assertEquals(daysWithHosts, test_days, 'incorrect days')
Test.assertEquals(hosts, [2582, 3222, 4190, 2502, 2537, 4106, 4406, 4317, 4523, 4346, 2864, 2650, 4454, 4214, 4340, 4385, 4168, 2550, 2560, 4134, 4456], 'incorrect hosts')

fig = plt.figure(figsize=(8,4.5), facecolor='white', edgecolor='white')
plt.axis([min(daysWithHosts), max(daysWithHosts), 0, max(hosts)+500])
plt.grid(color='grey', which='major', axis='y')
plt.xlabel('Day')
plt.ylabel('Hosts')
plt.plot(daysWithHosts, hosts)
plt.savefig("./result/3d.png")

# (3e) Exercise: Average Number of Daily Requests per Hosts
dayAndHostTuple = (access_logs
                   .map(lambda log: (log.date_time.day, log.host)))

groupedByDay = (dayAndHostTuple
                .groupByKey())

sortedByDay = (groupedByDay
               .sortByKey())

avgDailyReqPerHost = (sortedByDay
                      .map(lambda x: (x[0], len(x[1])/len(set(x[1]))))
                      .cache())

avgDailyReqPerHostList = (avgDailyReqPerHost
                          .take(30))
print('Hosts is %s' % avgDailyReqPerHostList)

# TEST Average number of daily requests per hosts (3e)
Test.assertEquals(avgDailyReqPerHostList, [(1, 13), (3, 12), (4, 14), (5, 12), (6, 12), (7, 13), (8, 13), (9, 14), (10, 13), (11, 14), (12, 13), (13, 13), (14, 13), (15, 13), (16, 13), (17, 13), (18, 13), (19, 12), (20, 12), (21, 13), (22, 12)], 'incorrect avgDailyReqPerHostList')
Test.assertTrue(avgDailyReqPerHost.is_cached, 'incorrect avgDailyReqPerHost.is_cache')

# (3f) Exercise: Visualizing the Average Daily Requests per Unique Host
daysWithAvg = avgDailyReqPerHost.map(lambda x: x[0]).collect()
avgs = avgDailyReqPerHost.map(lambda x: x[1]).collect()

print(daysWithAvg)
print(avgs)

# TEST Average Daily Requests per Unique Host (3f)
Test.assertEquals(daysWithAvg, [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22], 'incorrect days')
Test.assertEquals(avgs, [13, 12, 14, 12, 12, 13, 13, 14, 13, 14, 13, 13, 13, 13, 13, 13, 13, 12, 12, 13, 12], 'incorrect avgs')

fig = plt.figure(figsize=(8,4.2), facecolor='white', edgecolor='white')
plt.axis([0, max(daysWithAvg), 0, max(avgs)+2])
plt.grid(color='grey', which='major', axis='y')
plt.xlabel('Day')
plt.ylabel('Average')
plt.plot(daysWithAvg, avgs)
plt.savefig("./result/3e.png")

# (4a) Exercise: Counting 404 Response Codes
badRecords = (access_logs
              .filter(lambda log: log.response_code == 404)
              .cache())
print('Found %d 404 URLs' % badRecords.count())

# TEST Counting 404 (4a)
Test.assertEquals(badRecords.count(), 6185, 'incorrect badRecords.count()')
Test.assertTrue(badRecords.is_cached, 'incorrect badRecords.is_cached')

# (4b) Exercise: Listing 404 Response Code Records
badEndpoints = (badRecords
                .map(lambda log: log.endpoint))

badUniqueEndpoints = (badEndpoints
                      .distinct())

badUniqueEndpointsPick40 = (badUniqueEndpoints
                            .take(40))
print('404 URLS: %s' % badUniqueEndpointsPick40)


# TEST Listing 404 records (4b)

badUniqueEndpointsSet40 = set(badUniqueEndpointsPick40)
Test.assertEquals(len(badUniqueEndpointsSet40), 40, 'badUniqueEndpointsPick40 not distinct')

# (4c) Exercise: Listing the Top Twenty 404 Response Code Endpoints
badEndpointsCountPairTuple = (badRecords
                              .map(lambda log: (log.endpoint, 1)))

badEndpointsSum = (badEndpointsCountPairTuple
                   .reduceByKey(lambda a, b : a + b))

badEndpointsTop20 = (badEndpointsSum
                     .takeOrdered(20, lambda s: -1 * s[1]))
print('Top Twenty 404 URLs: %s' % badEndpointsTop20)

# TEST Top twenty 404 URLs (4c)
Test.assertEquals(badEndpointsTop20, [(u'/pub/winvn/readme.txt', 633), (u'/pub/winvn/release.txt', 494), (u'/shuttle/missions/STS-69/mission-STS-69.html', 431), (u'/images/nasa-logo.gif', 319), (u'/elv/DELTA/uncons.htm', 178), (u'/shuttle/missions/sts-68/ksc-upclose.gif', 156), (u'/history/apollo/sa-1/sa-1-patch-small.gif', 146), (u'/images/crawlerway-logo.gif', 120), (u'/://spacelink.msfc.nasa.gov', 117), (u'/history/apollo/pad-abort-test-1/pad-abort-test-1-patch-small.gif', 100), (u'/history/apollo/a-001/a-001-patch-small.gif', 97), (u'/images/Nasa-logo.gif', 85), (u'/shuttle/resources/orbiters/atlantis.gif', 64), (u'/history/apollo/images/little-joe.jpg', 62), (u'/images/lf-logo.gif', 59), (u'/shuttle/resources/orbiters/discovery.gif', 56), (u'/shuttle/resources/orbiters/challenger.gif', 54), (u'/robots.txt', 53), (u'/elv/new01.gif>', 43), (u'/history/apollo/pad-abort-test-2/pad-abort-test-2-patch-small.gif', 38)], 'incorrect badEndpointsTop20')

# (4d) Exercise: Listing the Top Twenty-five 404 Response Code Hosts
errHostsCountPairTuple = (badRecords
                          .map(lambda log: (log.host, 1)))

errHostsSum = (errHostsCountPairTuple
               .reduceByKey(lambda a, b : a + b))

errHostsTop25 = (errHostsSum
                 .takeOrdered(25, lambda s: -1 * s[1]))
print('Top 25 hosts that generated errors: %s' % errHostsTop25)

Test.assertEquals(len(errHostsTop25), 25, 'length of errHostsTop25 is not 25')
Test.assertEquals(len(set(errHostsTop25) - set([(u'maz3.maz.net', 39), (u'piweba3y.prodigy.com', 39), (u'gate.barr.com', 38), (u'm38-370-9.mit.edu', 37), (u'ts8-1.westwood.ts.ucla.edu', 37), (u'nexus.mlckew.edu.au', 37), (u'204.62.245.32', 33), (u'163.206.104.34', 27), (u'spica.sci.isas.ac.jp', 27), (u'www-d4.proxy.aol.com', 26), (u'www-c4.proxy.aol.com', 25), (u'203.13.168.24', 25), (u'203.13.168.17', 25), (u'internet-gw.watson.ibm.com', 24), (u'scooter.pa-x.dec.com', 23), (u'crl5.crl.com', 23), (u'piweba5y.prodigy.com', 23), (u'onramp2-9.onr.com', 22), (u'slip145-189.ut.nl.ibm.net', 22), (u'198.40.25.102.sap2.artic.edu', 21), (u'gn2.getnet.com', 20), (u'msp1-16.nas.mr.net', 20), (u'isou24.vilspa.esa.es', 19), (u'dial055.mbnet.mb.ca', 19), (u'tigger.nashscene.com', 19)])), 0, 'incorrect errHostsTop25')

# (4e) Exercise: Listing 404 Response Codes per Day
errDateCountPairTuple = (badRecords
                         .map(lambda log: (log.date_time.day, 1)))

errDateSum = (errDateCountPairTuple
              .reduceByKey(lambda a, b : a + b))

errDateSorted = (errDateSum
                 .sortByKey()
                 .cache())

errByDate = (errDateSorted.take(100))
print('404 Errors by day: %s' % errByDate)

# TEST 404 response codes per day (4e)
Test.assertEquals(errByDate, [(1, 243), (3, 303), (4, 346), (5, 234), (6, 372), (7, 532), (8, 381), (9, 279), (10, 314), (11, 263), (12, 195), (13, 216), (14, 287), (15, 326), (16, 258), (17, 269), (18, 255), (19, 207), (20, 312), (21, 305), (22, 288)], 'incorrect errByDate')
Test.assertTrue(errDateSorted.is_cached, 'incorrect errDateSorted.is_cached')

# (4f) Exercise: Visualizing the 404 Response Codes by Day
daysWithErrors404 = errDateSorted.map(lambda x: x[0]).collect()
errors404ByDay = errDateSorted.map(lambda x: x[1]).collect()

print(daysWithErrors404)
print(errors404ByDay)

# TEST Visualizing the 404 Response Codes by Day (4f)
Test.assertEquals(daysWithErrors404, [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22], 'incorrect daysWithErrors404')
Test.assertEquals(errors404ByDay, [243, 303, 346, 234, 372, 532, 381, 279, 314, 263, 195, 216, 287, 326, 258, 269, 255, 207, 312, 305, 288], 'incorrect errors404ByDay')

fig = plt.figure(figsize=(8,4.2), facecolor='white', edgecolor='white')
plt.axis([0, max(daysWithErrors404), 0, max(errors404ByDay)])
plt.grid(color='grey', which='major', axis='y')
plt.xlabel('Day')
plt.ylabel('404 Errors')
plt.plot(daysWithErrors404, errors404ByDay)
plt.savefig("./result/4f.png")

# (4g) Exercise: Top Five Days for 404 Response Codes
topErrDate = (errDateSorted
              .takeOrdered(5, lambda s: -1 * s[1]))
print('Top Five dates for 404 requests: %s' % topErrDate)

# TEST Five dates for 404 requests (4g)
Test.assertEquals(topErrDate, [(7, 532), (8, 381), (6, 372), (4, 346), (15, 326)], 'incorrect topErrDate')

# (4h) Exercise: Hourly 404 Response Codes
hourCountPairTuple = (badRecords
                      .map(lambda log: (log.date_time.hour, 1)))

hourRecordsSum = (hourCountPairTuple
                  .reduceByKey(lambda a, b : a + b))

hourRecordsSorted = (hourRecordsSum
                     .sortByKey()
                     .cache())

errHourList = (hourRecordsSorted
               .take(24))
print('Top hours for 404 requests: %s' % errHourList)

# TEST Hourly 404 response codes (4h)
Test.assertEquals(errHourList, [(0, 175), (1, 171), (2, 422), (3, 272), (4, 102), (5, 95), (6, 93), (7, 122), (8, 199), (9, 185), (10, 329), (11, 263), (12, 438), (13, 397), (14, 318), (15, 347), (16, 373), (17, 330), (18, 268), (19, 269), (20, 270), (21, 241), (22, 234), (23, 272)], 'incorrect errHourList')
Test.assertTrue(hourRecordsSorted.is_cached, 'incorrect hourRecordsSorted.is_cached')

# (4i) Exercise: Visualizing the 404 Response Codes by Hour
hoursWithErrors404 = hourRecordsSorted.map(lambda x: x[0]).collect()
errors404ByHours = hourRecordsSorted.map(lambda x: x[1]).collect()

print(hoursWithErrors404)
print(errors404ByHours)

# TEST Visualizing the 404 Response Codes by Hour (4i)
Test.assertEquals(hoursWithErrors404, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23], 'incorrect hoursWithErrors404')
Test.assertEquals(errors404ByHours, [175, 171, 422, 272, 102, 95, 93, 122, 199, 185, 329, 263, 438, 397, 318, 347, 373, 330, 268, 269, 270, 241, 234, 272], 'incorrect errors404ByHours')

fig = plt.figure(figsize=(8,4.2), facecolor='white', edgecolor='white')
plt.axis([0, max(hoursWithErrors404), 0, max(errors404ByHours)])
plt.grid(color='grey', which='major', axis='y')
plt.xlabel('Hour')
plt.ylabel('404 Errors')
plt.plot(hoursWithErrors404, errors404ByHours)
plt.savefig("./result/4i.png")
