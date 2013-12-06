#Note: I redirect the output of this file to "task_times_converted.txt"

#users.txt is just a list of unique users (sorted) from "finishing_tasks.txt"

g = open("users.txt", "r")

user_list = {}
count = 0
for line in g:
	user_list[line[:-1]] = count
	count+=1
g.close()

f = open("task_times.txt", "r")

for line in f:
	split = line.split(" ")
	index = user_list[split[4]]
	print split[0], split[1], split[2], split[3], index, split[5], split[6], split[7], split[8], split[9][:-1]
f.close()
