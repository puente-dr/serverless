import csv

def csvHandler(response):
    csv_file = response['Body'].read().decode('utf-8').splitlines()
    csv_reader = csv.reader(csv_file)
    next(csv_reader) # Skip header row
    data = []
    for row in csv_reader:
        data.append((row[0], row[1], row[2]))
    return data