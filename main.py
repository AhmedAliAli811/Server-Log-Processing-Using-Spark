with open('data/raw/access.log', 'r') as f:
    for line in f.readlines()[:5]:
        print(line.strip())


Ip , Timestamp , request_method, request_url, status_code, response_size, referrer, user_agent