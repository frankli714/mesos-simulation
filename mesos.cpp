#include <algorithm>
#include <vector>
#include <unordered_map>
#include <glog/logging.h>

#include "mesos.hpp"
#include "shared.hpp"

using namespace std;

void Framework::task_dependency_finished(
    Task& task, size_t finished_job_id, double time) {
  CHECK_GE(time, sets_updated_time) << "Time not monotonic";
  CHECK(future_tasks.find(task.id()) != future_tasks.end())
    << task.id() << " not in future_tasks";
  CHECK(task.remaining_dependencies.erase(finished_job_id) > 0);

  if (task.remaining_dependencies.empty()) {
    sets_updated_time = time;
    future_tasks.erase(task.id());
    upcoming_tasks.insert(task.id());
  }
}

void Framework::task_started(size_t task_id, double time) {
  CHECK_GE(time, sets_updated_time) << "Time not monotonic";
  CHECK(upcoming_tasks.find(task_id) != upcoming_tasks.end())
    << task_id << " not in upcoming_tasks";

  sets_updated_time = time;
  upcoming_tasks.erase(task_id);
  running_tasks.insert(task_id);
}

void Framework::task_finished(size_t task_id, double time) {
  CHECK_GE(time, sets_updated_time) << "Time not monotonic";
  CHECK(running_tasks.find(task_id) != running_tasks.end())
    << task_id << " not in upcoming_tasks";

  sets_updated_time = time;
  running_tasks.erase(task_id);
  finished_tasks.insert(task_id);
}

// We want to run the first task (and only the first thing) in each job.
// We can run all jobs in parallel.
// We can only run tasks if their start time has passed.
// TODO: revise this
vector<size_t> Framework::eligible_tasks(
    Indexer<Task>& tasks,
    const Indexer<Job>& jobs,
    double current_time) const {
  CHECK_GE(current_time, sets_updated_time);

  vector<size_t> result;
  for (const size_t& task_id : upcoming_tasks) {
    Task& task = tasks.get(task_id);
    if (task.start_time >= current_time)
      continue;
    result.push_back(task.id());
  }
  return result;

#if 0
  for (const deque<size_t>& task_list : task_lists) {
    if (task_list.empty()) continue;

    Task& task = tasks.get(task_list.front());
    if (task.being_run || task.start_time >= current_time)
      continue;

    // If we don't think dependencies have been met, then check once more.
    if (!task.dependencies_met) {
      task.dependencies_met = all_of(
          task.dependencies.begin(), task.dependencies.end(),
          [&](const size_t& job_id) {
            return jobs.get(job_id).finished();
          });
    }
    if (!task.dependencies_met) continue;

    result.push_back(task.id());
  }
  
  return result;
#endif
}

/* vim: set ts=2 sts=2 sw=2 tw=80 expandtab */
