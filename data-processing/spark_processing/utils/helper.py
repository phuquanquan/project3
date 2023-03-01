import re

# Regular expression to extract fields from log line
regex_pattern = '^(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\S+) (\S+|-)(?: "(.*?)")?(?: "(.*?)")?'

# Function to parse log line into fields
def parse_log_line(line):
    match = re.search(regex_pattern, line)
    if not match:
        return None
    return {
        'ip': match.group(1),
        'client': match.group(2),
        'user': match.group(3),
        'datetime': match.group(4),
        'request': match.group(5),
        'status': match.group(6),
        'size': match.group(7),
        'referer': match.group(8),
        'user_agent': match.group(9)
    }
