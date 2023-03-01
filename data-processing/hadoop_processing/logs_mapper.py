#!/usr/bin/env python

import sys
import re
from datetime import datetime

for line in sys.stdin:
    # Parse log line
    parts = line.strip().split(' ')
    ip = parts[0]
    client = parts[1]
    user = parts[2]
    datetime_obj = datetime.strptime(parts[3][1:], "%d/%b/%Y:%H:%M:%S")
    request = parts[4]
    status = parts[5]
    size = parts[6] if parts[6] != '-' else 0
    referer = parts[7] if len(parts) > 7 else '-'
    user_agent = parts[8] if len(parts) > 8 else '-'

    # Emit key-value pair
    print('{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}'.format(ip, client, user, datetime_obj.strftime("%Y-%m-%d %H:%M:%S"), request, status, size, referer, user_agent))
