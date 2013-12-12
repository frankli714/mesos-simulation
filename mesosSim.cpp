#include <algorithm>
#include <cassert>
#include <climits>
#include <cmath>
#include <iomanip>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <queue>
#include <set>
#include <stack>
#include <unordered_map>
#include <unordered_set>

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "auction.hpp"
#include "shared.hpp"
#include "simulation.hpp"
#include "mesos.hpp"

using namespace std;

// i have decided to go for a priority queue based simulation; the caveat is
// that code cant be directly implemented;

class MesosSimulation;

class OfferEvent : public Event<MesosSimulation> {
 public:
  OfferEvent(double etime) : Event(etime) {}
  virtual void run(MesosSimulation& sim);

  void run_drf(MesosSimulation& sim);
  void run_round_robin(MesosSimulation& sim);
};

class AuctionEvent : public Event<MesosSimulation> {
 public:
  AuctionEvent(double etime) : Event(etime) {}
  virtual void run(MesosSimulation& sim);
};

class StartTaskEvent: public Event<MesosSimulation> {
 public:
   StartTaskEvent(double etime, size_t framework_id) 
     : Event(etime),
     _framework_id(framework_id) {}

   virtual void run(MesosSimulation& sim);

 private:
   size_t _framework_id;
};

class FinishedTaskEvent : public Event<MesosSimulation> {
 public:
  FinishedTaskEvent(double etime, size_t slave_id,
                    size_t framework_id, size_t task_id,
                    size_t job_id)
      : Event(etime),
        _slave_id(slave_id),
        _framework_id(framework_id),
        _task_id(task_id),
        _job_id(job_id) {}

  virtual void run(MesosSimulation& sim);

 private:
  size_t _slave_id;
  size_t _framework_id;
  size_t _task_id;
  size_t _job_id;
};

enum Policy {
  ROUND_ROBIN,
  DRF,
  AUCTION
};

class MesosSimulation : public Simulation<MesosSimulation> {
 public:
  MesosSimulation(
      int _num_slaves, int _num_special_slaves,
      int _num_frameworks, Policy _policy);

  void use_resources(size_t slave, const Resources& resources);
  void release_resources(size_t slave, const Resources& resources);

  void start_task(size_t slave, Framework& f, Task& t, double time, double rent=0);

  void update_budget(Framework& f, double time);

  unordered_map<size_t, Resources> all_free_resources() const;
  void offer_resources(size_t framework_id,
                       unordered_map<size_t, Resources> resources, double now);

  Indexer<Slave> allSlaves;
  Indexer<Framework> allFrameworks;
  Indexer<Task> allTasks;
  Indexer<Job> allJobs;

  size_t round_robin_next_framework = 0;
  unordered_map<size_t, int> framework_num_tasks_available;
  // A mapping from jobs to the IDs of the tasks which depend on these jobs.
  unordered_multimap<size_t, size_t> job_dependents;

  const int num_slaves;
  const int num_special_slaves;
  const int num_frameworks;
  Policy policy;

  static Resources total_resources, used_resources;

  bool ready_for_auction;
  bool currently_making_offers;
};
Resources MesosSimulation::total_resources;
Resources MesosSimulation::used_resources;

DEFINE_int32(num_slaves, 10, "Number of slaves in simulation.");
DEFINE_int32(num_special_slaves, 0, "Number of special slaves in simulation");
DEFINE_int32(num_frameworks, 5, "Number of frameworks.");
DEFINE_string(policy, "drf", 
    "Resource allocation policy. One of roundrobin, drf, auction.");
DEFINE_string(trace, "synthetic_workload.txt",
    "Path to trace for trace_workload.");

void rand_workload(int num_frameworks, int num_jobs, int num_tasks_per_job,
                   Indexer<Framework>& allFrameworks, Indexer<Task>& allTasks,
                   Indexer<Job>& allJobs) {
  // This simply generates random tasks for debugging purposes
  srand(0);
  for (int i = 0; i < num_frameworks; i++) {
    Framework& framework = allFrameworks.add();
    for (int j = 0; j < num_jobs; j++) {
      allJobs.add(num_tasks_per_job);
      for (int k = 0; k < num_tasks_per_job; k++) {
        Task& t = allTasks.add();

        t.used_resources.cpus = static_cast<double>(rand() % 4);
        t.used_resources.mem = static_cast<double>(rand() % 5);
        t.used_resources.disk = 0;

        t.being_run = false;
        t.task_time = (double)(rand() % 10);
        framework.upcoming_tasks.insert(t.id());
      }
    }
  }
}

void trace_workload(
    int num_frameworks,
    Indexer<Framework>& allFrameworks,
    Indexer<Task>& allTasks,
    Indexer<Job>& allJobs,
    MesosSimulation* sim) {

  for (int i = 0; i < num_frameworks; i++) {
    Framework& framework = allFrameworks.add();

    if (sim->policy == AUCTION) {
      framework.budget = sim->num_slaves * 100.000 / num_frameworks;
      framework.budget_time = 0;
      framework.income = sim->num_slaves * 1.0 / num_frameworks;
      framework.expenses = 0;
    }
  }

  allJobs.add(0); // Dummy job with 0 tasks so that real jobs begin at ID 1.

  string line;
  ifstream trace(FLAGS_trace);
  CHECK(trace.is_open()) << FLAGS_trace << " not opened";

//  int job_vector_index = 0;
  size_t last_j_id = 0;
  while (getline(trace, line)) {
    // 0: job_id 1: task_index 2: start_time 3: end_time 4: framework_id 
    // 5: scheduling_class 6: priority 7: cpu 8: ram 9: disk
    // 10: special_resource_speedup 11-: dependencies
    vector<double> split_v = split(line, ' ');
    CHECK_GE(split_v.size(), 11) << "Not enough parts in line.";

    Task& task = allTasks.add();
    task.framework_id = static_cast<size_t>(split_v[4]);
    task.slave_id = 0;  //Set simply as default
    task.job_id = split_v[0];
    task.used_resources = { split_v[7], split_v[8], split_v[9] };
    task.being_run = false;
    task.task_time = split_v[3] - split_v[2];
    task.start_time = split_v[2];
    task.special_resource_speedup = split_v[10];
    // Add dependencies of task
    for(int i = 11; i < split_v.size(); i++) {
      CHECK_NE(split_v[i], task.job_id) << "Self-dependency not allowed";
      task.remaining_dependencies.insert(split_v[i]);
      sim->job_dependents.emplace(split_v[i], task.id());
    }

    CHECK_GE(task.task_time, 0) << "ERROR IN PROCESSING TASK TIME! " 
      << task.job_id << " " << task.start_time << " " << split_v[3] << endl;

    Framework& framework = allFrameworks[(size_t) split_v[4]];
    CHECK_EQ(framework.id(), split_v[4])
        << "Frameworks did not appear in order";
    // Create event for the start time of each task, so we know a new task is
    // eligible for a particular framework
    sim->add_event(new StartTaskEvent(task.start_time, framework.id()));
    // Add task to framework
    if (task.remaining_dependencies.empty()) {
      framework.upcoming_tasks.insert(task.id());
    } else {
      framework.future_tasks.insert(task.id());
    }

    Job* job;
    if (split_v[0] != last_j_id) {
      last_j_id = split_v[0];
      // Create a new job since we haven't seen this job yet.
      job = &allJobs.add(0);
      CHECK_EQ(job->id(), last_j_id);
    } else {
      job = &allJobs.get(last_j_id);
    }
    job->increment_tasks();
    if (task.start_time < job->start_time) {
      job->start_time = task.start_time;
    }
  }
  trace.close();
}

MesosSimulation::MesosSimulation(int _num_slaves, int _num_special_slaves, int _num_frameworks, Policy _policy)
  : num_slaves(_num_slaves), num_special_slaves(_num_special_slaves),
    num_frameworks(_num_frameworks), policy(_policy) {
  currently_making_offers = false;
  ready_for_auction = false;

  CHECK_GE(num_slaves, num_special_slaves);
  // lets initialize slave state and the event queue
  for (int i = 0; i < num_slaves; i++) {
    allSlaves.add();
    total_resources += allSlaves[i].resources;
  }
  for(int i = num_slaves - num_special_slaves; i < num_slaves; i++) {
    allSlaves[i].special_resource = true;
  }

  //rand_workload(num_frameworks, 2, 2, allFrameworks, allTasks);
  trace_workload(num_frameworks, allFrameworks, allTasks, allJobs, this);
//  cout << "Done generating workload" << endl;
  set_remaining_tasks(allTasks.size());

#if 0
  //Spot check
  int sum_size = 0;
  for (int i = 0; i < num_frameworks; i++) {
    for (int j = 0; j < allFrameworks[i].task_lists.size(); j++) {
      if (allFrameworks[i].task_lists[j].size() > 1) {
        for (int k = 1; k < allFrameworks[i].task_lists[j].size(); k++) {
          const Task& t1 = allTasks[allFrameworks[i].task_lists[j][k - 1]];
          const Task& t2 = allTasks[allFrameworks[i].task_lists[j][k]];
          if (t1.start_time + t1.task_time > t2.start_time) {
            cout << "ERROR: Spot check fail" << endl;
            exit(EXIT_FAILURE);
          }
        }
      }

      sum_size += allFrameworks[i].task_lists[j].size();
    }
  }
#endif

  if (DEBUG) {
    for (const auto& slave : allSlaves) {
      cout << "Slave " << slave.id()
           << " has total cpus: " << slave.resources.cpus
           << " mem: " << slave.resources.mem << endl;
    }
  }
}

void MesosSimulation::use_resources(size_t slave, const Resources& resources) {
  allSlaves[slave].free_resources -= resources;
  used_resources += resources;
  CHECK_LE(used_resources, total_resources);
  if (DEBUG)
    cout << "Using Slave " << slave << ": "
         << allSlaves[slave].free_resources.cpus << " "
         << allSlaves[slave].free_resources.mem << endl;
}

void MesosSimulation::release_resources(size_t slave,
                                        const Resources& resources) {
  allSlaves[slave].free_resources += resources;
  used_resources -= resources;
  Resources zero = {0,0,0};
  if(used_resources < zero){
    Resources small_neg = {-1*pow(10,-10),-1*pow(10,-10),-1*pow(10,-10)};
    CHECK_GT(used_resources, small_neg);
    used_resources = zero;
  }
  if (DEBUG)
    cout << "Using Slave " << slave << ": "
         << allSlaves[slave].free_resources.cpus << " "
         << allSlaves[slave].free_resources.mem << endl;
}

unordered_map<size_t, Resources> MesosSimulation::all_free_resources() const {
  unordered_map<size_t, Resources> result;
  for (const Slave& slave : allSlaves) {
    result[slave.id()] = slave.free_resources;
  }
  return result;
}

void MesosSimulation::start_task(
    size_t slave_id, Framework& f,
    Task& task, double now, double rent) {

    Slave& slave = allSlaves.get(slave_id);
    task.slave_id = slave_id;
    this->use_resources(slave_id, task.used_resources);

    VLOG(1) << "Scheduling Task " << task.id() << " on " << slave_id << " at " << now;

    static const Resources zero = {0,0,0};
    CHECK_GE(slave.free_resources, zero);

    task.being_run = true;
    f.current_used += task.used_resources;
    f.cpu_share = f.current_used.cpus / total_resources.cpus;
    f.mem_share = f.current_used.mem / total_resources.mem;
    f.disk_share = f.current_used.disk / total_resources.disk;
    f.dominant_share = max({
      f.cpu_share, f.mem_share, f.disk_share
    });

    CHECK_GT(framework_num_tasks_available.count(f.id()), 0);
    framework_num_tasks_available[f.id()] -= 1;
    CHECK_GE(framework_num_tasks_available[f.id()], 0);
    if (framework_num_tasks_available[f.id()] == 0) {
      framework_num_tasks_available.erase(f.id());
      CHECK_EQ(framework_num_tasks_available.count(f.id()), 0);
    }
   
    double task_runtime = task.task_time;
    if (slave.special_resource) task_runtime *= task.special_resource_speedup;	
    slave.curr_tasks.insert(task.id());
    this->add_event(new FinishedTaskEvent(
        now + task_runtime, task.slave_id, f.id(),
        task.id(), task.job_id));

    if (policy == AUCTION) {
      this->update_budget(f, now);
      VLOG(2) << f.expenses << " += " << task.rent;
      f.expenses += rent;
      task.rent = rent;
    }

    f.task_started(task.id(), now);

    if (DEBUG) {
      cout << "Slave scheduled: id=" << slave.id()
           << " cpu: " << slave.free_resources.cpus
           << " mem: " << slave.free_resources.mem << endl;
    }

}

void MesosSimulation::offer_resources(
    size_t framework_id, unordered_map<size_t, Resources> resources,
    double now) {
  cerr << "Framework " << framework_id << " offered:" << endl;
  vector<size_t> slaves;
  for (const auto& kv : resources) {
    slaves.push_back(kv.first);
  }
  sort(slaves.begin(), slaves.end());
  for (size_t slave_id : slaves) {
    cerr << "  slave " << slave_id << ": " << resources[slave_id] << " [ " << allSlaves[slave_id].special_resource << endl;
  }

  Framework& f = allFrameworks[framework_id];
  if (DEBUG) cout << "Framework " << framework_id << endl;

  vector<size_t> eligible_tasks = f.eligible_tasks(allTasks, allJobs, now);
  for (size_t task_id : eligible_tasks) {
    Task& todo_task = allTasks.get(task_id);
    if (DEBUG) {
      cout << "Task id=" << task_id << " : cpu "
           << todo_task.used_resources.cpus << " mem "
           << todo_task.used_resources.mem << " being_run "
           << todo_task.being_run << endl;
    }
    bool available_special_slave = false;
    if (todo_task.special_resource_speedup < 1) {
      for (auto& kv : resources) {
        const size_t slave_id = kv.first;
        Resources& slave_resources = kv.second;
        Slave& slave = allSlaves.get(slave_id);
        if (!slave.special_resource) continue;
        if (slave_resources >= todo_task.used_resources) {
          available_special_slave = true;
          break;
        }
      }
    }

    for (auto& kv : resources) {
      const size_t slave_id = kv.first;
      Resources& slave_resources = kv.second;
      Slave& slave = allSlaves.get(slave_id);
      if(available_special_slave && !slave.special_resource) continue;
        
      
      if (slave_resources >= todo_task.used_resources) {
        start_task(slave_id, f, todo_task, now);
        slave_resources -= todo_task.used_resources;
        break;
      }
    }
  }
}

unordered_map<double, bool> already_offered_framework;

void OfferEvent::run(MesosSimulation& sim) {
  if (DEBUG)
    cout << "Time is " << sim.get_clock() << " process_offer function" << endl;

  if (sim.policy == ROUND_ROBIN) {
    run_round_robin(sim);
  } else if (sim.policy == DRF) {
    run_drf(sim);
  }
  // Offer again in 1 second if we should be making offers anyways
  if (sim.currently_making_offers)
    sim.add_event(new OfferEvent(this->get_time() + 1000000));

  //cout << "Offers made at time " << << endl;
  //Log utilization
  cout << sim.get_clock() / 1000000 << " " << sim.used_resources.cpus << " "
       << sim.total_resources.cpus << " " << sim.used_resources.mem << " "
       << sim.total_resources.mem << " " << sim.used_resources.disk << " "
       << sim.total_resources.disk << endl;
/*  size_t idle = 0;
  for(int i = 0; i < sim.num_frameworks; i++) {
    vector<size_t> tmp = sim.allFrameworks[i].eligible_tasks(sim.allTasks, sim.jobs_to_tasks, sim.get_clock());
    idle += tmp.size();
  }
  cout << idle << " tasks waiting to be scheduled" << endl;
*/
}

void OfferEvent::run_round_robin(MesosSimulation& sim) {
  // Offer everything to the next framework
  size_t framework = sim.round_robin_next_framework++;
  sim.round_robin_next_framework &= sim.allFrameworks.size();
  sim.offer_resources(framework, sim.all_free_resources(), get_time());
}

void OfferEvent::run_drf(MesosSimulation& sim) {

  assert(sim.currently_making_offers);
  double min_dominant_share = 1.0;
  double next_id = 0;
  bool make_offer = false;
  for (auto it = sim.framework_num_tasks_available.begin();
     it != sim.framework_num_tasks_available.end();
     it++ ) {
    Framework& framework = sim.allFrameworks[it->first];
    assert(sim.framework_num_tasks_available.count(framework.id()) > 0);
    if ( already_offered_framework.count(framework.id()) > 0) continue;
    double dominant_share = framework.dominant_share;
    if (DEBUG) {
      cout << "DRF computes dominant share for framework " << framework.id()
           << " is " << dominant_share << endl;
    }
    if (dominant_share <= min_dominant_share) {
      make_offer = true;
      min_dominant_share = dominant_share;
      next_id = framework.id();
    }
  }
  if (DEBUG) cout << "DRF is offering next to framework " << next_id << endl;
  // Offer everything to the most dominant-resource-starved framework, 
  // only if there exists tasks to schedule
  if (make_offer) {
    already_offered_framework[next_id] = true;

    bool all_offered = true;
    for ( auto it = sim.framework_num_tasks_available.begin(); it != sim.framework_num_tasks_available.end(); it++) {
      if(already_offered_framework.count(it->first) == 0){
        all_offered = false;
        break;
      }
    }
    if (all_offered) {
      //cout << "All offers made after this round!" << endl;
      sim.currently_making_offers = false;
      already_offered_framework.clear();
    }

//    cout << "Making offer to " << next_id << endl;
    sim.offer_resources(next_id, sim.all_free_resources(), get_time());
  } else {
    sim.currently_making_offers = false;
  }
}

void MesosSimulation::update_budget(Framework& framework, double time){
  CHECK_GE(time, framework.budget_time);
  framework.budget +=
      (time - framework.budget_time) * (framework.income - framework.expenses) / 1000000;
  framework.budget_time = time;
}

void AuctionEvent::run(MesosSimulation& sim) {
  // Increment everyone's budget
  for (Framework& framework : sim.allFrameworks) {
    sim.update_budget(framework, get_time());
  }  
  // Collect:
  // - free resources
  unordered_map<size_t, Resources> free_resources = sim.all_free_resources();

  size_t num_bids = 0;
  // - bids
  unordered_map<FrameworkID, vector<vector<Bid>>> all_bids;
  for (const Framework& framework : sim.allFrameworks) {
    VLOG(1) << "Framework " << framework.id() <<" has..." << endl;
    VLOG(1) << "   Budget: " << framework.budget;
    VLOG(1) << " Expenses: " << framework.expenses;
    if (framework.budget < 0) continue;
    vector<vector<Bid>>& bids = all_bids[framework.id()];
    vector<size_t> tasks = framework.eligible_tasks(sim.allTasks, sim.allJobs, get_time());

    for (size_t task_id : tasks) {
      const Task& task = sim.allTasks.get(task_id);
      vector<Bid> bids_for_task;

      for (const auto& kv : free_resources) {
        num_bids++;
        const Resources& slave_resources = kv.second;
        if (slave_resources < task.used_resources) {
          continue;
        }

        // TODO: reconsider whether we need all this information here
        Bid bid;
        bid.framework_id = framework.id();
        bid.slave_id = kv.first;
        bid.task_id = task.id();
        bid.requested_resources = task.used_resources;

        double time_horizon = 1000000;
        double effective_income = framework.income - framework.expenses + framework.budget / time_horizon;
          
        bid.wtp = bid.requested_resources.weight() * effective_income / framework.income;
        if (sim.allSlaves[bid.slave_id].special_resource){
            bid.wtp /= task.special_resource_speedup;
        }
        bids_for_task.push_back(std::move(bid));
      }
      bids.push_back(std::move(bids_for_task));
      // TODO: if the task fit none of the free resources on the slaves, then
      // should we just give up (the current behavior)?
    }
  }

  // - reservation prices
  Resources reservation_price(0.1, 0.05, 0.01);
  // - minimum price increase
  double min_price_increase = 0;
  // - price multiplier
  double price_multiplier = 1.1;

  // Run the auction
  Auction auction(all_bids, free_resources, reservation_price,
                  min_price_increase, price_multiplier);
  auction.run();

  // Get the results
  const unordered_map<SlaveID, vector<Bid*>>& results = auction.results();

  // Stop making auctions, unless something gets sold this round
  sim.ready_for_auction = false;
  for (const auto& result : results) {
    size_t slave_id = result.first;
    const vector<Bid*>& winning_bids = result.second;

    for (const Bid* bid : winning_bids) {
      CHECK_EQ(slave_id, bid->slave_id);

      Framework& framework = sim.allFrameworks.get(bid->framework_id);
      sim.start_task(slave_id, framework, sim.allTasks.get(bid->task_id), get_time(), bid->current_price);
      sim.ready_for_auction = true;

    }
  }
    cout << sim.get_clock() / 1000000 << " " << sim.used_resources.cpus << " "
       << sim.total_resources.cpus << " " << sim.used_resources.mem << " "
       << sim.total_resources.mem << " " << sim.used_resources.disk << " "
       << sim.total_resources.disk << endl;

  // Run auction again in 1 second
  if (sim.ready_for_auction){
/*  size_t idle = 0;
  for(int i = 0; i < sim.num_frameworks; i++) {
    vector<size_t> tmp = sim.allFrameworks[i].eligible_tasks(sim.allTasks, sim.jobs_to_tasks, sim.get_clock());
    idle += tmp.size();
  }
  cout << idle << " tasks waiting to be scheduled" << endl;
  cout << num_bids << " bids made this auction" << endl;
*/

    sim.add_event(new AuctionEvent(get_time() + 1000000));
  }

}

double roundUp(double curr_time, double increments) {
  double remainder = fmod(curr_time, increments);
  if (remainder == 0)
    return curr_time;
  return curr_time + increments - remainder;
}

void StartTaskEvent::run(MesosSimulation& sim) {
  double f_id = this->_framework_id;
  Framework& f = sim.allFrameworks[f_id];
//  cout << "Starting task at time " << this->get_time() << " for framework " << f_id << endl;
  if(sim.framework_num_tasks_available.count(f.id()) == 0) {
    sim.framework_num_tasks_available[f.id()] = 1;
  } else {
    sim.framework_num_tasks_available[f.id()] += 1;
  }

  if (sim.policy == AUCTION){
    if (!sim.ready_for_auction){
      sim.add_event(new AuctionEvent( roundUp(this->get_time()+1, 1000000)));
      sim.ready_for_auction = true;
    }
  } else{
  //Restart making offers, because this new task may be schedulable
      if(!sim.currently_making_offers) {
        sim.currently_making_offers = true;
        sim.add_event(new OfferEvent( roundUp(this->get_time()+1, 1000000)));
      }
  }
}

void FinishedTaskEvent::run(MesosSimulation& sim) {
  auto& allFrameworks = sim.allFrameworks;
  auto& allTasks = sim.allTasks;
  auto& allSlaves = sim.allSlaves;

//  cout << "Finishing a task at " << this->get_time() << " for framework " << this->_framework_id << endl;

  double s_id = this->_slave_id;
  double t_id = this->_task_id;
  double f_id = this->_framework_id;
  double j_id = this->_job_id;

  Slave& slave = allSlaves[s_id];
  Framework& framework = allFrameworks[f_id];
  Task& task = allTasks[t_id];

  if (DEBUG) {
    cout << "Time is " << sim.get_clock() << " finished_task " << t_id << endl;
  }
  VLOG(0) << "Task " << t_id << " finished at time " << get_time();
  sim.release_resources(slave.id(), task.used_resources);
  if (sim.policy == AUCTION) {
    sim.update_budget(framework, get_time());
    framework.expenses -= task.rent;
  }
  framework.current_used -= task.used_resources;
  framework.cpu_share = framework.current_used.cpus / sim.total_resources.cpus;
  framework.mem_share = framework.current_used.mem / sim.total_resources.mem;
  framework.disk_share = framework.current_used.disk / sim.total_resources.disk;
  framework.dominant_share = max({
    framework.cpu_share, framework.mem_share, framework.disk_share
  });
  
  slave.curr_tasks.erase(t_id);

  framework.task_finished(t_id, this->get_time());

  sim.decrement_remaining_tasks();

  Job& job = sim.allJobs.get(j_id);
  job.complete_task();
  if (job.finished()) {
    job.end_time = this->get_time() - job.start_time;

    // Update dependents of job
    auto its = sim.job_dependents.equal_range(j_id);
    for (auto it = its.first; it != its.second; ++it) {
      Task& task = allTasks.get(it->second);
      Framework& framework = allFrameworks.get(task.framework_id);
      framework.task_dependency_finished(task, j_id, this->get_time());
    }
  }

  if (sim.policy == AUCTION){
      if(!sim.ready_for_auction){
        sim.add_event(new AuctionEvent( roundUp(this->get_time(), 1000000)));
        sim.ready_for_auction = true;
      }
  } else{
      if(!sim.currently_making_offers) {
        sim.currently_making_offers = true;
        sim.add_event(new OfferEvent( roundUp(this->get_time(), 1000000)));
      }
  }

}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  Policy policy;
  if (FLAGS_policy == "roundrobin") {
    policy = ROUND_ROBIN;
  } else if (FLAGS_policy == "drf") {
    policy = DRF;
  } else if (FLAGS_policy == "auction") {
    policy = AUCTION;
  } else {
    LOG(FATAL) << "Inavlid --policy: " << FLAGS_policy;
  }

  MesosSimulation sim(
      FLAGS_num_slaves, FLAGS_num_special_slaves, FLAGS_num_frameworks, policy);
  sim.run();

  //METRICS: Completion time
  //double sum = 0;
//  cout << "#JOB COMPLETION TIMES" << endl;
  for (int i = 0; i < sim.allJobs.size(); ++i) {
    //cout << kv.first << endl;
//    cout << sim.jobs_to_num_tasks[kv.first] << " " << kv.second.second << endl;
    //sum += it->second.second;
  }
  //if(DEBUG) cout << "Average job completition time is " <<
  //float(sum)/float(jobs_to_tasks.size()) << endl;

  return 0;
}
/* vim: set ts=2 sts=2 sw=2 tw=80 expandtab */
