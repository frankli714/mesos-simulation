#include <vector>

#include "mesos.hpp"
#include "shared.hpp"

using namespace std;

// We want to run the first task (and only the first thing) in each job.
// We can run all jobs in parallel.
// We can only run tasks if their start time has passed.
// TODO: revise this
vector<size_t> Framework::eligible_tasks(
    const Indexer<Task>& tasks,
    double current_time) {
  vector<size_t> result;

  for (const deque<size_t>& task_list : task_lists) {
    if (task_list.size() == 0)
     continue;
   
    const Task& candidate_task = tasks.get(task_list.front());
    if (candidate_task.being_run || candidate_task.start_time > current_time)
      continue;

    result.push_back(candidate_task.id());
  }

  return result;
}

