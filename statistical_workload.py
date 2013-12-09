from collections import defaultdict
from random import *

cutoffs = [0.5, 2, 8, 64, 256, 1024, float('inf')]
machine_counts = [
        (6732, (.5, .5, 1)), 
        (3863, (.5, .25, 1)), 
        (1001, (.5, .75, 1)), 
        (795, (1, 1, 1)), 
        (126, (.25, .25, 1)), 
        (52, (.5, .12, 1))]

def buckets(jobs):
    task_sizes_by_bucket = { x : [] for x in cutoffs }
    counts_by_bucket = { x : 0 for x in cutoffs }
    for v in jobs.values():
        for x, y in zip(cutoffs[:-1], cutoffs[1:]):
            if x <= v[0] and v[0] < y:
                counts_by_bucket[x] += 1
                task_sizes_by_bucket[x].append(v[1:])
    return (task_sizes_by_bucket, counts_by_bucket)

def sample_tasks(task_sizes_by_bucket, counts_by_bucket):
    bucket = cutoffs[weighted_sample(counts_by_bucket[c] for c in cutoffs)]
    task_sizes = task_sizes_by_bucket[bucket]
    task_size = null_entry()
    for i in range(randint(1,3)):
        task_size = average(task_size, tuple([randint(1,10)] + choice(task_sizes)))
    task_size[0] = max(1, int((1 + 3 * random()) * bucket))
    return tasks_from_size(task_size)

def tasks_from_size(size):
    result = []
    for i in range(size[0]):
        temp = list(size)
        temp[1] = choice(temp[1])
        result.append(temp)
    return result

def weighted_sample(xs):
    xs = list(xs)
    total = sum(xs)
    t = total * random()
    s = 0
    i = 0
    while True:
        s += xs[i]
        if s > t:
            return i
        i += 1

def generate_tasks(jobs, frameworks, n):
    sizes, counts = buckets(jobs)
    for c in cutoffs:
        if c > n:
            counts[c] = 0
    result = []
    id = 0
    if type(frameworks) == int:
        frameworks = range(frameworks)
    while len(result) < n:
        framework = choice(frameworks)
        result += [ [id, framework] + task[1:] for task in sample_tasks(sizes, counts) ]
        id += 1
    return result[:n]

def print_tasks(tasks, filename="synthetic_workload.txt"):
    with open(filename, 'w') as f:
        for t in tasks:
            f.write(' '.join(str(x) for x in t) + '\n')

def generate_machines(m):
    total = sum(x[0] for x in machine_counts)
    fractions = [x * 1.0 / total for x in machine_counts]
    counts = [int(m*frac) for frac in fractions]
    residue = m - sum(lower_shares)
    for i in range(residue):
        counts[weighted_sample(fractions)]+=1
    return { machine_counts[i][1] : counts[i] for i in range(len(counts)) }

def average(t1, t2):
    n = t1[0]
    x = t1[1:]
    m = t2[0]
    y = t2[1:]
    return [n+m] + [x[0] + y[0]] + [ (n*a+m*b) * 1.0 / (n+m) for (a, b) in zip (x[1:], y[1:]) ]

def null_entry():
    return [0] + [[]] + [0 for field in ['cpu', 'ram', 'disk']]

def populate_jobs(filename="task_times_converted.txt", samples = float('inf')):
    with open(filename, 'r') as f:
        jobs = defaultdict(null_entry)
        k = 0
        for line in f:
            task_attributes = line.split(" ")
            job_id = task_attributes[0]
            task_resources = (1, [float(task_attributes[3]) - float(task_attributes[2])], float(task_attributes[7]), float(task_attributes[8]), float(task_attributes[9]))
            jobs[job_id] = average(jobs[job_id], task_resources)
            k += 1
            if k > samples:
                return jobs
    return jobs
