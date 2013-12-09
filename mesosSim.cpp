#include <algorithm>
#include <cassert>
#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <queue>
#include <set>
#include <stack>
#include <unordered_map>
#include <unordered_set>

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
   StartTaskEvent(double etime, unsigned int framework_id) 
     : Event(etime),
     _framework_id(framework_id) {}

   virtual void run(MesosSimulation& sim);

 private:
   unsigned int _framework_id;
};

class FinishedTaskEvent : public Event<MesosSimulation> {
 public:
  FinishedTaskEvent(double etime, unsigned int slave_id,
                    unsigned int framework_id, unsigned int task_id,
                    unsigned int job_id)
      : Event(etime),
        _slave_id(slave_id),
        _framework_id(framework_id),
        _task_id(task_id),
        _job_id(job_id) {}

  virtual void run(MesosSimulation& sim);

 private:
  unsigned int _slave_id;
  unsigned int _framework_id;
  unsigned int _task_id;
  unsigned int _job_id;
};

class MesosSimulation : public Simulation<MesosSimulation> {
 public:
  MesosSimulation();

  void use_resources(size_t slave, const Resources& resources);
  void release_resources(size_t slave, const Resources& resources);

  unordered_map<size_t, Resources> all_free_resources() const;
  void offer_resources(size_t framework_id,
                       unordered_map<size_t, Resources> resources, double now);

  Indexer<Slave> allSlaves;
  Indexer<Framework> allFrameworks;
  Indexer<Task> allTasks;

  static size_t round_robin_next_framework;

  unordered_map<unsigned int, pair<int, double>> jobs_to_tasks;
  unordered_map<unsigned int, int> jobs_to_num_tasks;
  unordered_map<unsigned int, int> framework_num_tasks_available;

  static const int num_slaves;
  static const int num_frameworks;
  static int max_job_id;

  static Resources total_resources, used_resources;

  bool currently_making_offers;
};

size_t MesosSimulation::round_robin_next_framework = 0;
const int MesosSimulation::num_slaves = 100;
const int MesosSimulation::num_frameworks = 397;
int MesosSimulation::max_job_id = 0;
Resources MesosSimulation::total_resources;
Resources MesosSimulation::used_resources;

// Functions handling events
void run_auction(const Event<MesosSimulation>& e);

void rand_workload(int num_frameworks, int num_jobs, int num_tasks_per_job,
                   Indexer<Framework>& allFrameworks, Indexer<Task>& allTasks) {
  // This simply generates random tasks for debugging purposes
  srand(0);
  for (int i = 0; i < num_frameworks; i++) {
    Framework& framework = allFrameworks.add();
    for (int j = 0; j < num_jobs; j++) {
      deque<size_t> q;
      for (int k = 0; k < num_tasks_per_job; k++) {
        Task& t = allTasks.add();

        t.used_resources.cpus = static_cast<double>(rand() % 4);
        t.used_resources.mem = static_cast<double>(rand() % 5);
        t.used_resources.disk = 0;

        t.being_run = false;
        t.task_time = (double)(rand() % 10);
        q.push_back(t.id());
      }
      framework.task_lists.push_back(q);
    }
  }
}

void trace_workload(
    int num_frameworks, int& max_job_id, Indexer<Framework>& allFrameworks,
    Indexer<Task>& allTasks,
    unordered_map<unsigned int, pair<int, double>>& jobs_to_tasks,
    unordered_map<unsigned int, int>& jobs_to_num_tasks,
    MesosSimulation* sim) {

  for (int i = 0; i < num_frameworks; i++) {
    Framework& framework = allFrameworks.add();
    framework.budget = 100;
    framework.budget_time = 5637000000;
    framework.budget_increase_rate = 100;
  }

  string line;
  ifstream trace("task_times_converted.txt");
  //ifstream trace("short_traces.txt");
  if (trace.is_open()) {
    cout << " IN " << endl;
    int job_vector_index = 0;
    double last_j_id = 0;
    while (getline(trace, line)) {
      // 0: job_id 1: task_index 2: start_time 3: end_time 4: framework_id 5:
      // scheduling_class
      // 6: priority 7: cpu 8: ram 9: disk
      vector<double> split_v = split(line, ' ');
      if (split_v[0] != last_j_id) {
        max_job_id++;
      }

      Task& t = allTasks.add();
      t.slave_id = 0;  //Set simply as default
      t.job_id = max_job_id;

      t.used_resources = { split_v[7], split_v[8], split_v[9] };
      t.being_run = false;
      t.task_time = split_v[3] - split_v[2];
      t.start_time = split_v[2];
      if (t.task_time <= 0) {
        cout << "ERROR IN PROCESSING TASK TIME! " << t.job_id << " "
             << t.start_time << " " << split_v[3] << endl;
        exit(EXIT_FAILURE);
      }

      Framework& f = allFrameworks[(int) split_v[4]];
      assert(f.id()==(int) split_v[4]);
      //Create event for the start time of each task, so we know a new task is
      //eligible for a particular framework
      sim->add_event(new StartTaskEvent(t.start_time, f.id()));

      if (split_v[0] != last_j_id) {
        last_j_id = split_v[0];
        job_vector_index = f.task_lists.size();
        deque<size_t> q;
        q.push_back(t.id());
        f.task_lists.push_back(q);
        jobs_to_tasks[t.job_id] = make_pair(1, t.start_time);
        jobs_to_num_tasks[t.job_id] = 1;

      } else {
        jobs_to_tasks[t.job_id].first += 1;
        jobs_to_num_tasks[t.job_id] += 1;

        if (t.start_time < jobs_to_tasks[t.job_id].second)
          jobs_to_tasks[t.job_id].second = t.start_time;
        int i;
        for (i = job_vector_index; i < f.task_lists.size(); i++) {
          bool has_intersection = false;
          for (int j = 0; j < f.task_lists[i].size(); j++) {
            const Task& cmp_task = allTasks[f.task_lists[i][j]];
            has_intersection = intersect(
                t.start_time, t.start_time + t.task_time, cmp_task.start_time,
                cmp_task.start_time + cmp_task.task_time);
            if (has_intersection) break;
          }
          if (!has_intersection) {
            if (DEBUG) cout << "No intersections! " << line << endl;
            int j;
            for (j = 0; j < f.task_lists[i].size(); j++) {
              if (t.start_time + t.task_time <
                  allTasks[f.task_lists[i][j]].start_time) {
                f.task_lists[i].insert(f.task_lists[i].begin() + j, t.id());
                break;
              }
            }
            if (j == f.task_lists[i].size()) {
              f.task_lists[i].push_back(t.id());
            }
            break;
          }
        }
        if (i == f.task_lists.size()) {
          deque<size_t> q;
          q.push_back(t.id());
          f.task_lists.push_back(q);
        }
      }
    }
    trace.close();
  }
}

MesosSimulation::MesosSimulation() {
  currently_making_offers = false;

  // lets initialize slave state and the event queue
  for (int i = 0; i < num_slaves; i++) {
    allSlaves.add();
    total_resources += allSlaves[i].resources;
  }

  //rand_workload(num_frameworks, 2, 2, allFrameworks, allTasks);
  trace_workload(num_frameworks, max_job_id, allFrameworks, allTasks,
                 jobs_to_tasks, jobs_to_num_tasks, this);
  cout << "Done generating workload" << endl;
  set_remaining_tasks(allTasks.size());

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
  cout << "DONE " << sum_size << endl;
  //add_event(new OfferEvent(5637000000));

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
  assert(used_resources <= total_resources);
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
    assert(used_resources > small_neg);
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

void MesosSimulation::offer_resources(
    size_t framework_id, unordered_map<size_t, Resources> resources,
    double now) {

  Framework& f = allFrameworks[framework_id];
  if (DEBUG) cout << "Framework " << framework_id << endl;

  vector<size_t> eligible_tasks = f.eligible_tasks(allTasks, now);
  for (size_t task_id : eligible_tasks) {
    Task& todo_task = allTasks.get(task_id);
    if (DEBUG) {
      cout << "Task id=" << task_id << " : cpu "
           << todo_task.used_resources.cpus << " mem "
           << todo_task.used_resources.mem << " being_run "
           << todo_task.being_run << endl;
    }

    for (auto& kv : resources) {
      const size_t slave_id = kv.first;
      Resources& slave_resources = kv.second;
      Slave& slave = allSlaves.get(slave_id);

      if (slave_resources >= todo_task.used_resources) {
        if (DEBUG) cout << "Should schedule" << endl;
        todo_task.slave_id = slave_id;
        this->use_resources(slave_id, todo_task.used_resources);
        slave_resources -= todo_task.used_resources;

        Resources zero = {0,0,0};
        assert(slave.free_resources >= zero);

        todo_task.being_run = true;
        f.current_used += todo_task.used_resources;
        f.cpu_share = f.current_used.cpus / total_resources.cpus;
        f.mem_share = f.current_used.mem / total_resources.mem;
        f.disk_share = f.current_used.disk / total_resources.disk;
        f.dominant_share = max({
          f.cpu_share, f.mem_share, f.disk_share
        });
        assert(framework_num_tasks_available.count(f.id()) > 0);
        framework_num_tasks_available[f.id()] -= 1;
        assert(framework_num_tasks_available[f.id()] >= 0);
        if (framework_num_tasks_available[f.id()] == 0) {
          framework_num_tasks_available.erase(f.id());
          assert(framework_num_tasks_available.count(f.id()) == 0);
        }

        slave.curr_tasks.insert(todo_task.id());
        this->add_event(new FinishedTaskEvent(
            now + todo_task.task_time, todo_task.slave_id, framework_id,
            todo_task.id(), todo_task.job_id));

        if (DEBUG) {
          cout << "Slave scheduled: id=" << slave.id()
               << " cpu: " << slave.free_resources.cpus
               << " mem: " << slave.free_resources.mem << endl;
        }
        break;
      }
    }
  }
}

unordered_map<double, bool> already_offered_framework;

void OfferEvent::run(MesosSimulation& sim) {
  if (DEBUG)
    cout << "Time is " << sim.get_clock() << " process_offer function" << endl;

#if defined RR
  run_round_robin(sim);
#elif defined DRF
  run_drf(sim);
#endif
  // Offer again in 1 second if we should be making offers anyways
  if (sim.currently_making_offers)
    sim.add_event(new OfferEvent(this->get_time() + 1000000));

  //cout << "Offers made at time " << << endl;
  //Log utilization
  cout << sim.get_clock() / 1000000 << " " << sim.used_resources.cpus << " "
       << sim.total_resources.cpus << " " << sim.used_resources.mem << " "
       << sim.total_resources.mem << " " << sim.used_resources.disk << " "
       << sim.total_resources.disk << endl;
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
  double now = get_time();
  bool make_offer = false;
  for (const Framework& framework : sim.allFrameworks) {
    if (already_offered_framework.count(framework.id()) > 0) continue;
    double dominant_share = framework.dominant_share;
    if (DEBUG) {
      cout << "DRF computes dominant share for framework " << framework.id()
           << " is " << dominant_share << endl;
    }
    if (dominant_share <= min_dominant_share &&
        sim.framework_num_tasks_available.count(framework.id()) > 0) {
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
      cout << "All offers made!" << endl;
      sim.currently_making_offers = false;
      already_offered_framework.clear();
    }

    cout << "Making offer to " << next_id << endl;
    sim.offer_resources(next_id, sim.all_free_resources(), get_time());
  } else {
    sim.currently_making_offers = false;
  }
}

void AuctionEvent::run(MesosSimulation& sim) {
  // Increment everyone's budget
  for (Framework& framework : sim.allFrameworks) {
    assert(get_time() >= framework.budget_time);
    framework.budget +=
        (get_time() - framework.budget_time) * framework.budget_increase_rate;
  }

  // Collect:
  // - free resources
  unordered_map<size_t, Resources> free_resources = sim.all_free_resources();

  // - bids
  unordered_map<FrameworkID, vector<vector<Bid>>> all_bids;
  for (const Framework& framework : sim.allFrameworks) {
    vector<vector<Bid>>& bids = all_bids[framework.id()];
    vector<size_t> tasks = framework.eligible_tasks(sim.allTasks, get_time());

    for (size_t task_id : tasks) {
      const Task& task = sim.allTasks.get(task_id);
      vector<Bid> bids_for_task;
      for (const auto& kv : free_resources) {
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

        bid.wtp = framework.budget / 10;  // FIXME
        bids_for_task.push_back(std::move(bid));
      }
      bids.push_back(std::move(bids_for_task));
      // TODO: if the task fit none of the free resources on the slaves, then
      // should we just give up (the current behavior)?
    }
  }

  // - reservation prices
  Resources reservation_price(10, 10, 10);
  // - minimum price increase
  double min_price_increase = 1;
  // - price multiplier
  double price_multiplier = 1.1;

  // Run the auction
  Auction auction(all_bids, free_resources, reservation_price,
                  min_price_increase, price_multiplier, true);
  auction.run();

  // Get the results
  const unordered_map<SlaveID, vector<Bid*>>& results = auction.results();

  // Reorganize the results
  //            framework id          slave id
  //            v                     v
  unordered_map<size_t, unordered_map<size_t, Resources>>
      resources_per_framework_per_slave;
  for (const auto& result : results) {
    size_t slave_id = result.first;
    const vector<Bid*>& winning_bids = result.second;

    for (const Bid* bid : winning_bids) {
      assert(slave_id == bid->slave_id);
      resources_per_framework_per_slave[bid->framework_id][slave_id] +=
          bid->requested_resources;

      Framework& framework = sim.allFrameworks.get(bid->framework_id);
      framework.budget -= bid->current_price;
    }
  }

  // Print log
  for (const string& line : auction.get_log()) {
    cerr << line << endl;
  }

  // Implement the results
  for (const auto& kv : resources_per_framework_per_slave) {
    size_t framework_id = kv.first;
    const unordered_map<size_t, Resources>& resources_per_slave = kv.second;
    sim.offer_resources(framework_id, resources_per_slave, get_time());
  }

  // Run auction again in 1 second
  sim.add_event(new AuctionEvent(get_time() + 1000000));

  cerr << "Press Enter to continue..." << endl;
  cin.get();
}

double roundUp(double curr_time, double increments) {
  double remainder = fmod(curr_time, increments);
  if (remainder == 0)
    return curr_time;
  return curr_time + increments - remainder;
}

void StartTaskEvent::run(MesosSimulation& sim) {
  unsigned int f_id = this->_framework_id;
  Framework& f = sim.allFrameworks[f_id];
  cout << "Starting task at time " << this->get_time() << " for framework " << f_id << endl;
  if(sim.framework_num_tasks_available.count(f.id()) == 0) {
    sim.framework_num_tasks_available[f.id()] = 1;
  } else {
    sim.framework_num_tasks_available[f.id()] += 1;
  }

  //Restart making offers, because this new task may be schedulable
  if(!sim.currently_making_offers) {
    sim.currently_making_offers = true;
    sim.add_event(new OfferEvent( roundUp(this->get_time(), 1000000)));
  }
}

void FinishedTaskEvent::run(MesosSimulation& sim) {
  auto& allFrameworks = sim.allFrameworks;
  auto& allTasks = sim.allTasks;
  auto& allSlaves = sim.allSlaves;
  auto& jobs_to_tasks = sim.jobs_to_tasks;

  cout << "Finishing a task at " << this->get_time() << " for framework " << this->_framework_id << endl;

  unsigned int s_id = this->_slave_id;
  unsigned int t_id = this->_task_id;
  unsigned int f_id = this->_framework_id;
  unsigned int j_id = this->_job_id;

  Slave& slave = allSlaves[s_id];
  Framework& framework = allFrameworks[f_id];
  Task& task = allTasks[t_id];

  if (DEBUG) {
    cout << "Time is " << sim.get_clock() << " finished_task " << t_id << endl;
  }
  sim.release_resources(slave.id(), task.used_resources);
  framework.current_used -= task.used_resources;
  framework.cpu_share = framework.current_used.cpus / sim.total_resources.cpus;
  framework.mem_share = framework.current_used.mem / sim.total_resources.mem;
  framework.disk_share = framework.current_used.disk / sim.total_resources.disk;
  framework.dominant_share = max({
    framework.cpu_share, framework.mem_share, framework.disk_share
  });
  
  slave.curr_tasks.erase(t_id);

  for (deque<size_t>& task_list : framework.task_lists) {
    if (task_list.size() > 0 && task_list[0] == t_id) {
      assert(allTasks[task_list[0]].being_run);
      task_list.pop_front();
      break;
    }
  }

  sim.decrement_remaining_tasks();

  jobs_to_tasks[j_id].first -= 1;
  if (jobs_to_tasks[j_id].first == 0) {
    double start = jobs_to_tasks[j_id].second;
    jobs_to_tasks[j_id].second = this->get_time() - start;
  }
  
  //Restart making offers, in the case that a task can be scheduled now
  if(!sim.currently_making_offers) {
    sim.currently_making_offers = true;
    sim.add_event(new OfferEvent( roundUp(this->get_time(), 1000000)));
  }

}

int main(int argc, char* argv[]) {
  MesosSimulation sim;
  sim.run();

  //METRICS: Completion time
  //unsigned int sum = 0;
  cout << "#JOB COMPLETION TIMES" << endl;
  for (const auto& kv : sim.jobs_to_tasks) {
    cout << sim.jobs_to_num_tasks[kv.first] << " " << kv.second.second << endl;
    //sum += it->second.second;
  }
  //if(DEBUG) cout << "Average job completition time is " <<
  //float(sum)/float(jobs_to_tasks.size()) << endl;

  return 0;
}
/* vim: set ts=2 sts=2 sw=2 tw=80 expandtab */
