import sys
import json

with open(sys.argv[1], 'r') as file:
    data = json.load(file)

item = data["throughput"]
print(item)
