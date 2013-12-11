#ifndef __MESOS_HPP__
#define __MESOS_HPP__
// Defines classes representing components in Mesos (master, slaves, frameworks,
// tasks, etc.)

#include <vector>
#include <unordered_set>

#include "shared.hpp"

#include <deque>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <utility>

struct Task : public Indexable {
  Task() : rent(0), being_run(false) {}

  double slave_id;
  double job_id;
  Resources used_resources;
  double rent;
  double task_time;
  double start_time;
  bool being_run;
  double special_resource_speedup;
  std::vector<double> dependencies;
};

class Slave : public Indexable {
 public:
  Slave()
      : resources({
    1, 1, 1
  }),
        free_resources({
    1, 1, 1
  }),
  special_resource(false) {}
  Resources resources;
  std::unordered_set<size_t> curr_tasks;
  Resources free_resources;
  bool special_resource;
};

class Framework : public Indexable {
 public:
  Framework() : cpu_share(0), mem_share(0), disk_share(0), dominant_share(0) {}
  // Return a list of tasks which we can start running at the current time.
  std::vector<size_t> eligible_tasks(const Indexer<Task>& tasks,
                                     const std::unordered_map<double, std::pair<int, double>>& jobs_to_tasks,
                                     double current_time) const;

  std::vector<std::deque<size_t>> task_lists;
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
