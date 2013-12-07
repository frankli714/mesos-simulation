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

struct Task : public Indexable {
  unsigned int slave_id;
  unsigned int job_id;
  Resources used_resources;
  double task_time;
  double start_time;
  bool being_run;
};

class Slave : public Indexable {
 public:
  Slave() : resources({1, 1, 1}), free_resources({1, 1, 1}) {}
  Resources resources;
	std::unordered_set<size_t> curr_tasks;
  Resources free_resources;
};

class Framework : public Indexable {
 public:
	 Framework() : cpu_share(0), mem_share(0), disk_share(0), dominant_share(0) {}
	 // Return a list of tasks which we can start running at the current time.
	 std::vector<size_t> eligible_tasks(
			 const Indexer<Task>& tasks, double current_time) const;

	std::vector<std::deque<size_t>> task_lists;
	Resources current_used;

	double cpu_share, mem_share, disk_share, dominant_share;

	// The "current" budget of this process.
	double budget;
	// Time at which the budget was valid.
	double budget_time;
	// Amount of budget the framework receives per unit time.
	double budget_increase_rate;
};

#endif
