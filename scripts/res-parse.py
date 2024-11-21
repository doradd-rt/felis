import sys

thro = []
lacy = []

with open(sys.argv[1], 'r') as file:
    i = 0
    for line in file:
        line = line.strip()

        if (i % 3 == 1):
            if line.isdigit():
                thro.append(int(line))
            else:
                i -= 1  # Adjust index to recheck this line as a setting
        elif (i % 3 == 2):
            lacy.append(int(str(line.strip().split(",")[0][1:])))
        i+=1

for i in range(len(thro)):
    r_thro = round(float(thro[i]) / 1000000, 2)
    #r_lacy = round(float(lacy[i])/10000, 1)
    r_lacy = round(float(lacy[i])/1000, 1)
    print(r_thro, r_lacy)
