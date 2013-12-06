#Note: I redirect the output of this script to "task_times.txt"

#"finishing_tasks.txt" contains a list of all tasks from jobs that finish
f = open("finishing_tasks.txt", "r")
jobs_dict = {}

for line in f:
	(time, missing, job_id, task_index, machine_id, event_type, username, scheduling_class, priority, cpu, ram, disk, constraint) = line.split(',')
	job_id = int(job_id)
	task_index = int(task_index)
	if int(event_type) in [0,7,8]:
		continue
	if job_id not in jobs_dict:
		jobs_dict[job_id] = {}
	if task_index not in jobs_dict[job_id]:
		if int(event_type)==1:
			jobs_dict[job_id][task_index] = [time, 0, username,  scheduling_class, priority, cpu, ram, disk]
		else:
			#This means the task has not just started, and hence we will ignore it (we want success runtimes)
			continue
	else:
		if int(event_type)==1:
			jobs_dict[job_id][task_index][0] = time
		elif int(event_type)==4:
			jobs_dict[job_id][task_index][1] = time
		else:
			#This means the task has not succeeded, and hence we will ignore it (we want success runtimes)
			jobs_dict[job_id].pop(task_index)

f.close()

job_ids = jobs_dict.keys()
job_ids.sort()

for ids in job_ids:
	task_indices = jobs_dict[ids].keys()
	task_indices.sort()
	for index in task_indices:
		t = jobs_dict[ids][index]
		if len(t) != 8:
			print "ERROR: ", ids, index, t
			exit()
		if t[0]==0 or t[1]==0 or (t[0] > t[1]):
			#We didn't see both correct start and finish events, hence we do not consider this event.
			continue
		print ids, index, t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7] 
