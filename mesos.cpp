#include <vector>
#include <unordered_map>

#include "mesos.hpp"
#include "shared.hpp"

using namespace std;

// We want to run the first task (and only the first thing) in each job.
// We can run all jobs in parallel.
// We can only run tasks if their start time has passed.
// TODO: revise this
vector<size_t> Framework::eligible_tasks(
    const Indexer<Task>& tasks,
    const unordered_map<size_t, pair<int, double>>& jobs_to_tasks,
    double current_time) const {
  vector<size_t> result;

  for (const deque<size_t>& task_list : task_lists) {
    if (task_list.size() == 0) continue;

    const Task& candidate_task = tasks.get(task_list.front());
    if (candidate_task.being_run || candidate_task.start_time > current_time)
      continue;
    bool dependencies_met = true;
    for (double j_id : candidate_task.dependencies) {
      if(jobs_to_tasks.at(j_id).first != 0) {
        dependencies_met = false;
      }
    }
    if (!dependencies_met) continue;

    result.push_back(candidate_task.id());
  }
  
  return result;
}

/* vim: set ts=2 sts=2 sw=2 tw=80 expandtab */
