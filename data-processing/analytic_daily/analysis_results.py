class AnalysisResults:
    def __init__(self):
        self.num_requests = 0
        self.num_successful_requests = 0
        self.num_failed_requests = 0
        self.http_codes = {}
        self.http_methods = {}
        self.ip_address_access_count = {}
        self.http_method_success_count = {}
        self.http_method_failure_count = {}
        self.ip_address_success_count = {}
        self.ip_address_failure_count = {}
        self.total_response_time = 0.0
        self.num_successful_requests_with_response_time = 0

    def update_num_requests(self, n):
        self.num_requests += n

    def update_num_successful_requests(self, n):
        self.num_successful_requests += n

    def update_num_failed_requests(self, n):
        self.num_failed_requests += n

    def update_http_code(self, code, n):
        if code in self.http_codes:
            self.http_codes[code] += n
        else:
            self.http_codes[code] = n

    def update_http_method(self, method, n):
        if method in self.http_methods:
            self.http_methods[method] += n
        else:
            self.http_methods[method] = n

    def update_ip_address_access_count(self, ip_address, n):
        if ip_address in self.ip_address_access_count:
            self.ip_address_access_count[ip_address] += n
        else:
            self.ip_address_access_count[ip_address] = n

    def update_http_method_success_count(self, method, n):
        if method in self.http_method_success_count:
            self.http_method_success_count[method] += n
        else:
            self.http_method_success_count[method] = n

    def update_http_method_failure_count(self, method, n):
        if method in self.http_method_failure_count:
            self.http_method_failure_count[method] += n
        else:
            self.http_method_failure_count[method] = n

    def update_ip_address_success_count(self, ip_address, n):
        if ip_address in self.ip_address_success_count:
            self.ip_address_success_count[ip_address] += n
        else:
            self.ip_address_success_count[ip_address] = n

    def update_ip_address_failure_count(self, ip_address, n):
        if ip_address in self.ip_address_failure_count:
            self.ip_address_failure_count[ip_address] += n
        else:
            self.ip_address_failure_count[ip_address] = n

    def update_total_response_time(self, response_time):
        self.total_response_time += response_time

    def update_num_successful_requests_with_response_time(self, n):
        self.num_successful_requests_with_response_time += n
    
    def count_ip_address_accesses(self, parsed_data):
        """
        Count the number of accesses from each IP address.
        """
        for _, request in parsed_data:
            ip_address = request['ip_address']
            self.update_ip_address_access_count(ip_address, 1)
    
    def count_successful_and_failed_requests_per_ip(self, parsed_data):
        """
        Count the number of successful and failed requests for each IP address.
        """
        for _, request in parsed_data:
            ip_address = request['ip_address']
            if request['status_code'] >= 200 and request['status_code'] < 400:
                self.update_ip_address_success_count(ip_address, 1)
            else:
                self.update_ip_address_failure_count(ip_address, 1)
    
    def count_successful_and_failed_requests_per_http_method(self, parsed_data):
        http_methods = ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"]
        results = {}
        for method in http_methods:
            success_count = 0
            failure_count = 0
            for request in parsed_data:
                if request['method'] == method:
                    if request['status_code'] >= 200 and request['status_code'] < 400:
                        success_count += 1
                    else:
                        failure_count += 1
            results[method] = {'success': success_count, 'failure': failure_count}
        return results
    
    def calculate_http_response_percentage(self):
        """
        Calculate the percentage of HTTP responses.
        """
        total_requests = self.num_successful_requests + self.num_failed_requests
        if total_requests == 0:
            return {}
        http_response_percentages = {}
        for code, count in self.http_codes.items():
            http_response_percentages[code] = round((count / total_requests) * 100, 2)
        return http_response_percentages

    def calculate_average_and_total_response_time(self):
        """
        Calculate the average and total response time for successful and failed requests.
        """
        if self.num_successful_requests_with_response_time == 0:
            return {'total': 0.0, 'average': 0.0}
        average_response_time = self.total_response_time / self.num_successful_requests_with_response_time
        return {'total': self.total_response_time, 'average': round(average_response_time, 2)}

