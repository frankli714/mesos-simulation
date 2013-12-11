from collections import defaultdict
import argparse
import pickle
from random import *
import sys

cutoffs = [0.5, 2, 8, 64, 256, 1024, float('inf')]
machine_counts = [
        (6732, (.5, .5, 1)), 
        (3863, (.5, .25, 1)), 
        (1001, (.5, .75, 1)), 
        (795, (1, 1, 1)), 
        (126, (.25, .25, 1)), 
        (52, (.5, .12, 1))]

def generate_statistics(jobs):
    jobs_by_bucket = { x : [] for x in cutoffs }
    counts_by_bucket = { x : 0 for x in cutoffs }
    for j in jobs.values():
        for x, y in zip(cutoffs[:-1], cutoffs[1:]):
            if x <= j['count'] and j['count'] < y:
                counts_by_bucket[x] += 1
                jobs_by_bucket[x].append(j)
    return {'jobs': jobs_by_bucket, 'counts':counts_by_bucket}

def sample_tasks(jobs_by_bucket, counts_by_bucket):
    bucket = cutoffs[weighted_sample(counts_by_bucket[c] for c in cutoffs)]
    jobs = jobs_by_bucket[bucket]
    job = null_entry()
    for i in range(randint(1,3)):
        sample = dict(choice(jobs))
        sample['count'] = randint(1, 10)
        job = average(job, sample)
    size = max(1, int((1+3*random()) * bucket))
    return [sample_task(job) for i in range(size)]

def sample_task(job):
    result = dict(job)
    del result['times']
    del result['count']
    result['time'] = choice(job['times'])
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

def generate_tasks(statistics, frameworks, n):
    jobs = statistics['jobs']
    counts = statistics['counts']
    for c in cutoffs:
        if c > n:
            counts[c] = 0
    result = []
    job_id = 1
    task_id = 0
    if type(frameworks) == int:
        frameworks = range(frameworks)
    while len(result) < n:
        framework = choice(frameworks)
        tasks = sample_tasks(jobs, counts)
        for task in tasks:
            task['task_id'] = task_id
            task['job_id'] = job_id
            task['framework'] = framework
            task_id += 1
            result.append(task)
            if len(result) >= n:
                break
        job_id += 1
    return result[:n]

def print_tasks(tasks, filename="synthetic_workload.txt", num_users =1):
    # hack: we have users*frameworks frameworks, and then only collapse them for printing
    counts = defaultdict(lambda : 0)
    last_job = defaultdict(list)
    current_job = {}
    with open(filename, 'w') as f:
        for t in tasks:
            if t['framework'] in current_job and current_job[t['framework']] != t['job_id']:
                last_job[t['framework']] = [current_job[t['framework']]]
            current_job[t['framework']] = t['job_id']
# The appearance of task_id is just as a time
            serialized = [ t['job_id'], counts[t['job_id']], 0, t['time'], t['framework'] / num_users, 0, 0, t['cpu'], t['ram'], t['disk'], 1] + last_job[t['framework']]
            counts[t['job_id']] += 1
            f.write(' '.join(str(x) for x in serialized) + '\n')

def generate_machines(m):
    total = sum(x[0] for x in machine_counts)
    fractions = [x * 1.0 / total for x in machine_counts]
    counts = [int(m*frac) for frac in fractions]
    residue = m - sum(lower_shares)
    for i in range(residue):
        counts[weighted_sample(fractions)]+=1
    return { machine_counts[i][1] : counts[i] for i in range(len(counts)) }

def average(t1, t2):
    n = t1['count']
    m = t2['count']
    result = {}
    result['count'] = n+m
    result['times'] = t1['times'] + t2['times']
    for r in resources:
        result[r] = (n*t1[r] + m*t2[r]) * 1.0 / (n+m)
    return result

resources = ['cpu', 'ram', 'disk']

def null_entry():
    result = { r : 0 for r in resources + ['count'] }
    result['times'] = []
    return result

def populate_jobs(samples = float('inf'), filename=None):
    if filename is None:
        filename = default_data_source()
    with open(filename, 'r') as f:
        jobs = defaultdict(null_entry)
        k = 0
        for line in f:
            t = line.split(" ")
            job_id = t[0]
            task_resources = { 'count':1, 'times':[float(t[3]) - float(t[2])], 'cpu':float(t[7]), 'ram':float(t[8]), 'disk':float(t[9]) }
            jobs[job_id] = average(jobs[job_id], task_resources)
            k += 1
            if k > samples:
                return jobs
    return jobs

def default_data_source():
    return "task_times_converted.txt"

def cache_statistics(samples, filename=None):
    if filename is None:
        filename = default_data_source()
    cache = 'cached_statistics_{}_{}'.format(samples, filename)
    try:
        with open(cache, 'r') as f:
            statistics = pickle.load(f)
        assert( 'counts' in statistics and 'jobs' in statistics )
        assert( all (c in statistics['counts'] for c in cutoffs) )
    except Exception:
        print("Recomputing statistics...")
        statistics = generate_statistics(populate_jobs(samples, filename))
        with open(cache, 'w') as f:
            pickle.dump(statistics, f)
    return statistics

helpstring = "Generate statistical summary of trace and use it to synthesize comparable workload. "
helpstring += "Output is one task per line, space-separated: jobid, frameworkid, time, cpu, memory, disk."

parser = argparse.ArgumentParser(description=helpstring)
parser.add_argument('num_tasks', metavar='tasks', type=int, help='number of tasks to generate')
parser.add_argument('num_frameworks', metavar='frameworks', type=int, help='number of frameworks to assign tasks to')
parser.add_argument('num_users', metavar='users', type=int, help='number of independent threads on each framework')
parser.add_argument('--samples', metavar='s', type=int, help='number of samples to use to generate statistics [default=200000, max=1000000]', default=1000000)
parser.add_argument('--output', metavar='o', type=str, help='filename to store result [default=synthetic_workload.txt]', default='synthetic_workload.txt')
parser.add_argument('--input', metavar='i', type=str, help='trace to use as input [default={}]'.format(default_data_source()), default=default_data_source())
args = parser.parse_args()

statistics = cache_statistics(args.samples, args.input)
print_tasks(generate_tasks(statistics, args.num_frameworks*args.num_users, args.num_tasks), args.output, num_users=args.num_users)
