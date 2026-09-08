with open('data/raw/access.log', 'r') as f:
    for line in f.readlines()[:5]:
        print(line.strip())