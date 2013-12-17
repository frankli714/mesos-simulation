#ifndef __MESOS_HPP__
#define __MESOS_HPP__
// Defines classes representing components in Mesos (master, slaves, frameworks,
// tasks, etc.)

#include "shared.hpp"

#include <deque>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <utility>
#include <set>

struct Job : public Indexable {
  Job(size_t num_tasks, double start_time = 0)
      : num_remaining_tasks(num_tasks), num_total_tasks(num_tasks) {}

  void increment_tasks() {
      ++num_remaining_tasks;
      ++num_total_tasks;
  }
  
  void complete_task() {
      --num_remaining_tasks;
  }

  bool finished() const {
      return num_remaining_tasks == 0;
  }
  size_t num_remaining_tasks;
  size_t num_total_tasks;
  double start_time;
  double end_time;

#if 0
  unordered_set<size_t> tasks;
  // Jobs which have to complete before this job can start.
  std::unordered_set<size_t> prerequisites;
  // Jobs which cannot start before this job finishes.
  std::unordered_set<size_t> dependents;
#endif
};

struct Task : public Indexable {
  Task() : rent(0), being_run(false) {}

  size_t framework_id;
  size_t slave_id;
  size_t job_id;
  Resources used_resources;
  double rent;
  double task_time;
  double start_time;
  bool being_run;
  double special_resource_speedup;
  std::unordered_set<size_t> remaining_dependencies;
};

DECLARE_double(cpu);
DECLARE_double(mem);
DECLARE_double(disk);

class Slave : public Indexable {
 public:
  Slave() : resources({ FLAGS_cpu, FLAGS_mem, FLAGS_disk }),
            free_resources({ FLAGS_cpu, FLAGS_mem, FLAGS_disk }){};
  Resources resources;
  std::unordered_set<size_t> curr_tasks;
  Resources free_resources;
  bool special_resource;
};

class Framework : public Indexable {
 public:
  Framework() 
    : sets_updated_time(0), cpu_share(0), mem_share(0), disk_share(0),
      dominant_share(0) {}
  // Return a list of tasks which we can start running at the current time.
  std::vector<size_t> eligible_tasks(
            Indexer<Task>& tasks,
            const Indexer<Job>& jobs,
            double current_time) const;
    
  void task_dependency_finished(
          Task& task, size_t finished_job_id, double time);
  void task_started(size_t task_id, double time);
  void task_finished(size_t task_id, double time);

  // The time at which these sets were updated.
  double sets_updated_time;
  // Tasks which have finished.
  std::unordered_set<size_t> finished_tasks;
  // Tasks which are currently running.
  std::unordered_set<size_t> running_tasks;
  // Tasks which have uncompleted dependencies.
  std::unordered_set<size_t> future_tasks;
  // Tasks which have completed dependencies.
  std::set<size_t> upcoming_tasks;

  Resources current_used;

  double cpu_share, mem_share, disk_share, dominant_share;

  // The "current" budget of this process.
  double budget;
  // Time at which the budget was valid.
  double budget_time;
  // Amount of budget the framework receives per unit time.
  double income;
  // Amount framework is currently spending resources.
  double expenses;
};

#endif
/* vim: set ts=2 sts=2 sw=2 tw=80 expandtab */
