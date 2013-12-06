#ifndef __MESOS_HPP__
#define __MESOS_HPP__
// Defines classes representing components in Mesos (master, slaves, frameworks,
// tasks, etc.)

#include "shared.hpp"

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
  unordered_set<size_t> curr_tasks;
  Resources free_resources;
};

class Framework : public Indexable {
 public:
  vector<deque<size_t>> task_lists;
  Resources current_used;
};

#endif
