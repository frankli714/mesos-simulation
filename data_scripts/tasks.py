#Finishing_jobs is a list of all jobs that completed w/ signal 4 (success)

#Note: I redirect the output of this file to "finishing_tasks.txt"
f = open("finishing_jobs.txt","r")
jobs_dict = {}
for job_id in f:
	jobs_dict[int(job_id)]=1
f.close()

for i in xrange(1,500):
	str_i = str(i)
	pad = "0"*(3-len(str_i))
	filename = "part-00"+pad+str_i+"-of-00500.csv"
	g = open("task_events/"+filename, "r")
	for lines in g:
		split = lines.split(',')
		if int(split[2]) in jobs_dict:
			#Task is from finishing job
			print lines
	g.close()
