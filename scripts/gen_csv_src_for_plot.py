import csv
import sys

res_file = sys.argv[1]

with open(res_file, 'r') as f:
    num_lines = sum(1 for _ in f)
    print(num_lines)

with open(res_file, 'r') as f:
    idx = 0
    lines = f.readlines()
    curve_cnt =  6 # int(num_lines / 15)
    #per_curve_cnt = int(num_lines / curve_cnt)
    per_curve_cnt = 10
    curve_idx = 0
    curves = []
    curve_names = []
    print(curve_cnt)
    for i in range(curve_cnt):
        curve = [[], []]
        for j in range(per_curve_cnt - 1):
            idx = i * per_curve_cnt + j
            if (j == 0):
                curve_names.append(lines[idx].strip().split("-")[0][2:])
            else:
                split_ele = lines[idx].strip().split(" ")
                if (len(split_ele) != 2): continue
                thro, lacy = split_ele
                curve[0].append(float(thro))
                curve[1].append(float(lacy))
        curves.append(curve)

    print(curves)
    print(curve_names)

rows = []

# output csv should be in format - thro, lacy, thro, lacy (6 pairs)
for i in range(len(curves[0][0])): # number of rows
    row = []
    # walk through all curves
    for j in range(len(curves)):
        row.append(curves[j][0][i])
        row.append(round(curves[j][1][i] * 1000))
    rows.append(row)

csv_prefix = sys.argv[1].split(".")[0]
csv_name = csv_prefix + '.csv'

with open(csv_name, 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['throughput', 'Caracal ES=100', 'throughput', 'Caracal ES=500','throughput', 'Caracal ES=1000', 'throughput', 'Caracal ES=5000', 'throughput', 'Caracal ES=10000', 'throughput', 'Caracal ES=20000'])
    csvwriter.writerows(rows)
