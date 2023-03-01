#!/usr/bin/env python

import sys
import re
from datetime import datetime

for line in sys.stdin:
    # Parse log line
    parts = line.strip().split(' ')
    host = parts[0]
    client_identd = parts[1]
    user_id = parts[2]
    date_time = datetime.strptime(parts[3][1:], "%d/%b/%Y:%H:%M:%S")
    method = parts[4]
    endpoint = parts[5]
    protocol = parts[6] if parts[6] != '-' else 0
    response_code = parts[7] if len(parts) > 7 else '-'
    content_size = parts[8] if len(parts) > 8 else '-'

    # Emit key-value pair
    print('{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}'.format(host, client_identd, user_id, date_time.strftime("%Y-%m-%d %H:%M:%S"), method, endpoint, protocol, response_code, content_size))
