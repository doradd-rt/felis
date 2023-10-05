import sys

def read_data(file_path):
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            count, value = line.strip().split()
            data.append((int(count), int(value)))
    return data

def calculate_cdf(data):
    total_count = sum(count for count, _ in data)
    cdf = []
    cumulative_sum = 0
    for count, value in data:
        cumulative_sum += count
        cdf.append((value, cumulative_sum / total_count))
    return cdf

def calculate_percentiles(cdf):
    p99 = next(value for value, cumulative_prob in cdf if cumulative_prob >= 0.99)
    p90 = next(value for value, cumulative_prob in cdf if cumulative_prob >= 0.9)
    p50 = next(value for value, cumulative_prob in cdf if cumulative_prob >= 0.5)
    return p99, p90, p50

if __name__ == "__main__":
   data_input = read_data(sys.argv[1])
   cdf = calculate_cdf(data_input)
   print(calculate_percentiles(cdf))
