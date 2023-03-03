class AnalysisResults:
    def __init__(self):
        self.num_requests = 0
        self.num_successful_requests = 0
        self.num_failed_requests = 0
        self.content_sizes_avg = 0
        self.content_size_min = 0
        self.content_size_max = 0
        self.unique_host_count = 0
        self.bad_records = 0
        self.http_codes = {}
        self.host_more_than_10 = {}
        self.endpoints = {}
        self.top_ten_rrr_URLs = {} 
        self.daily_hosts_list = {}
        self.avg_daily_req_per_host_list  = {}
        self.bad_unique_endpoints_pick_40 = {}
        self.bad_endpoints_top_20 = {}
        self.err_hosts_top_25 = {}
        self.err_by_date = {}
        self.err_hour_list = {}

    def update_num_requests(self, n):
        self.num_requests += n

    def update_num_successful_requests(self, n):
        self.num_successful_requests += n

    def update_num_failed_requests(self, n):
        self.num_failed_requests += n

    def update_content_sizes_avg(self, n):
        self.content_sizes_avg += n

    def update_content_sizes_min(self, n):
        self.content_size_min += n

    def update_content_sizes_max(self, n):
        self.content_size_max += n

    def update_unique_host_count(self, n):
        self.unique_host_count += n

    def update_bad_records(self, n):
        self.bad_records += n

    def update_http_code(self, code_counts):
        for code, count in code_counts:
            if code in self.http_codes:
                self.http_codes[code] += count
            else:
                self.http_codes[code] = count

    def update_host_more_than_10(self, host_counts):
        for host, count in host_counts:
            if host in self.host_more_than_10:
                self.host_more_than_10[host] += count
            else:
                self.host_more_than_10[host] = count

    def update_endpoints(self, endpoints):
        for endpoint, count in endpoints:
            if endpoint in self.host_more_than_10:
                self.host_more_than_10[endpoint] += count
            else:
                self.host_more_than_10[endpoint] = count

    def update_top_ten_rrr_URLs(self, topTenErrURLs):
        for endpoint, count in topTenErrURLs:
            if endpoint in self.top_ten_rrr_URLs:
                self.top_ten_rrr_URLs[endpoint] += count
            else:
                self.top_ten_rrr_URLs[endpoint] = count

    def update_daily_hosts(self, dailyHostsList):
        for day, host in dailyHostsList:
            if day in self.daily_hosts_list:
                self.daily_hosts_list[day] += host
            else:
                self.daily_hosts_list[day] = host

    def update_avg_daily_req_per_host_list(self, avgDailyReqPerHostList):
        for day, avg_host in avgDailyReqPerHostList:
            if day in self.avg_daily_req_per_host_list:
                self.avg_daily_req_per_host_list[day] += avg_host
            else:
                self.avg_daily_req_per_host_list[day] = avg_host

    def update_bad_unique_endpoints_pick_40(self, badUniqueEndpointsPick40):
        for i, badUniqueEndpoints in badUniqueEndpointsPick40:
            self.bad_unique_endpoints_pick_40[i] = badUniqueEndpoints

    def update_bad_endpoints_top_20(self, badEndpointsTop20):
        for endpoint, count in badEndpointsTop20:
            if endpoint in self.bad_endpoints_top_20:
                self.bad_endpoints_top_20[endpoint] += count
            else:
                self.bad_endpoints_top_20[endpoint] = count

    def update_err_hosts_top_25(self, errHostsTop25):
        for host, count in errHostsTop25:
            if host in self.err_hosts_top_25:
                self.err_hosts_top_25[host] += count
            else:
                self.err_hosts_top_25[host] = count

    def update_err_by_date(self, errByDate):
        for day, count in errByDate:
            if day in self.err_by_date:
                self.err_by_date[day] += count
            else:
                self.err_by_date[day] = count

    def update_err_hour_list(self, errHourList):
        for day, count in errHourList:
            if day in self.err_hour_list:
                self.err_hour_list[day] += count
            else:
                self.err_hour_list[day] = count