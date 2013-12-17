import sys

a = []

with open(sys.argv[1], 'r') as f:
    for x in f:
        xs = x.split(" ")
        a.append([int(xs[0]), float(xs[1]) / float(xs[2])])

s = 0
for i in range(1, len(a)):
    s += (a[i][0] - a[i-1][0]) * a[i][1]

print(s / (a[-1][0]  - a[0][0]))
