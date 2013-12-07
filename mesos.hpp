#ifndef __MESOS_HPP__
#define __MESOS_HPP__
// Defines classes representing components in Mesos (master, slaves, frameworks,
// tasks, etc.)

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
	 // Return a list of tasks which we can start running at the current time.
	 std::vector<size_t> eligible_tasks(
			 const Indexer<Task>& tasks, double current_time);

	std::vector<std::deque<size_t>> task_lists;
  Resources current_used;
};

#endif
