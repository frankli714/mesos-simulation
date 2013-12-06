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

//#define RR
#define DRF

class MesosSimulation;

class OfferEvent : public Event<MesosSimulation> {
 public:
  OfferEvent(double etime) : Event(etime) {}
  virtual void run(MesosSimulation& sim);
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

  Indexer<Slave> allSlaves;
  Indexer<Framework> allFrameworks;
  Indexer<Task> allTasks;

  unordered_map<unsigned int, pair<int, unsigned int> > jobs_to_tasks;
  unordered_map<unsigned int, int> jobs_to_num_tasks;

  static const int num_slaves;
  static const int num_frameworks;
  static int max_job_id;

  static Resources total_resources, used_resources;
};

const int MesosSimulation::num_slaves = 10;
const int MesosSimulation::num_frameworks = 397;
int MesosSimulation::max_job_id = 0;
Resources MesosSimulation::total_resources;
Resources MesosSimulation::used_resources;

// Functions handling events
void run_auction(const Event<MesosSimulation> &e);

void rand_workload(
   int num_frameworks,
   int num_jobs,
   int num_tasks_per_job,
   Indexer<Framework>& allFrameworks,
   Indexer<Task>& allTasks) {
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
   int num_frameworks,
   int& max_job_id,
   Indexer<Framework>& allFrameworks,
   Indexer<Task>& allTasks,
   unordered_map<unsigned int, pair<int, unsigned int>>& jobs_to_tasks,
   unordered_map<unsigned int, int>& jobs_to_num_tasks) {

  for (int i = 0; i < num_frameworks; i++) {
    Framework& f = allFrameworks.add();
  }

  string line;
  ifstream trace("task_times_converted.txt");
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
              if (t.start_time + t.task_time 
                  < allTasks[f.task_lists[i][j]].start_time) {
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
  // lets initialize slave state and the event queue
  for (int i = 0; i < num_slaves; i++) {
    allSlaves.add();
    total_resources += allSlaves[i].resources;
  }

  //rand_workload(num_frameworks, 2, 2, allFrameworks, allTasks);
  trace_workload(num_frameworks, max_job_id, allFrameworks, allTasks, jobs_to_tasks, jobs_to_num_tasks);
  cout << "Done generating workload" << endl;

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

  add_event(new OfferEvent(5637000000));

  if (DEBUG) {
    for (const auto& slave : allSlaves) {
      cout << "Slave " << slave.id()
           << " has total cpus: " << slave.resources.cpus
           << " mem: " << slave.resources.mem << endl;
    }
  }
}

int main(int argc, char *argv[]) {
  MesosSimulation sim;
  sim.run(2506181000000);

  //METRICS: Completion time
  unsigned int sum = 0;
  cout << "#JOB COMPLETION TIMES" << endl;
  for (const auto& kv : sim.jobs_to_tasks) {
    cout << sim.jobs_to_num_tasks[kv.first] << " " << kv.second.second << endl;
    //sum += it->second.second;
  }
  //if(DEBUG) cout << "Average job completition time is " <<
  //float(sum)/float(jobs_to_tasks.size()) << endl;

  return 0;
}

void use_resources(MesosSimulation& sim, Slave *s, Resources r) {
  s->free_resources -= r;
  sim.total_resources += r;
  if (DEBUG)
    cout << "Using Slave " << s->id() << ": " << s->free_resources.cpus << " "
         << s->free_resources.mem << endl;
}

void release_resources(MesosSimulation& sim, Slave *s, Resources r) {
  s->free_resources += r; 
  sim.used_resources -= r;
  if (DEBUG)
    cout << "Freeing Slave " << s->id() << ": " << s->free_resources.cpus << " "
         << s->free_resources.mem << endl;

}

unsigned int curr_framework_offer = 0;

void round_robin(MesosSimulation& sim) {
  curr_framework_offer++;
  curr_framework_offer %= sim.num_frameworks;
}

void drf(MesosSimulation& sim) {
  auto& allFrameworks = sim.allFrameworks;
  auto& allTasks = sim.allTasks;
  auto& allSlaves = sim.allSlaves;

  double max_share = 1.0;
  double next_id = 0;
  for (int i = 0; i < allFrameworks.size(); i++) {
    const Framework& f = allFrameworks[i];
    double cpu_share = f.current_used.cpus / sim.total_resources.cpus;
    double mem_share = f.current_used.mem / sim.total_resources.mem;
    double disk_share = f.current_used.disk / sim.total_resources.disk;
    double dominant_share = max({cpu_share, mem_share, disk_share});
    if (DEBUG) {
      cout << "DRF computes dominant share for framework " << i << " is "
           << dominant_share << endl;
    }
    if (dominant_share < max_share) {
      max_share = dominant_share;
      next_id = i;
    }
  }
  if (DEBUG) cout << "DRF is offering next to framework " << next_id << endl;
  curr_framework_offer = next_id;
}

void OfferEvent::run(MesosSimulation& sim) {
  if (DEBUG) cout << "Time is " << sim.get_clock() << " process_offer function" << endl;

#ifdef RR
  round_robin(sim);
#endif
#ifdef DRF
  drf(sim);
#endif

  auto& allFrameworks = sim.allFrameworks;
  auto& allTasks = sim.allTasks;
  auto& allSlaves = sim.allSlaves;

  Framework& f = allFrameworks[curr_framework_offer];
  if (DEBUG) cout << "Framework " << curr_framework_offer << endl;

  for (int i = 0; i < f.task_lists.size(); ++i) {
    deque<size_t>& task_list = f.task_lists[i];
    if (task_list.size() == 0 || allTasks[task_list[0]].being_run ||
        allTasks[task_list[0]].start_time > this->get_time()) {
      if (DEBUG) cout << "No task to currently run in thread " << i << endl;
      continue;
    }

    Task& todo_task = allTasks.get(task_list[0]);
    if (DEBUG) {
      cout << "Task id=" << task_list[0] << " : cpu "
           << todo_task.used_resources.cpus << " mem "
           << todo_task.used_resources.mem << " being_run "
           << todo_task.being_run << endl;
    }

    for (Slave& slave : allSlaves) {
      if (slave.free_resources >= todo_task.used_resources) {
        if (DEBUG) cout << "Should schedule" << endl;
        todo_task.slave_id = slave.id();
        use_resources(sim, &slave, todo_task.used_resources);

        todo_task.being_run = true;
        f.current_used += todo_task.used_resources;

        slave.curr_tasks.insert(todo_task.id());
        sim.add_event(
            new FinishedTaskEvent(this->get_time() + todo_task.task_time,
              todo_task.slave_id, curr_framework_offer, todo_task.id(),
              todo_task.job_id));

        if (DEBUG) {
          cout << "Slave scheduled: id=" << slave.id()
               << " cpu: " << slave.free_resources.cpus
               << " mem: " << slave.free_resources.mem << endl;
        }
        break;
      }
    }
  }

  sim.add_event(new OfferEvent(this->get_time() + 60000000));

  //cout << "Offers made at time " << << endl;
  //Log utilization
  cout << sim.get_clock() / 1000000 << " " << sim.used_resources.cpus << " " << sim.total_resources.cpus
       << " " << sim.used_resources.mem << " " << sim.total_resources.mem 
       << " " << sim.used_resources.disk << " " << sim.total_resources.disk << endl;
}

void FinishedTaskEvent::run(MesosSimulation& sim) {
  auto& allFrameworks = sim.allFrameworks;
  auto& allTasks = sim.allTasks;
  auto& allSlaves = sim.allSlaves;
  auto& jobs_to_tasks = sim.jobs_to_tasks;

  unsigned int s_id = this->_slave_id;
  unsigned int t_id = this->_task_id;
  unsigned int f_id = this->_framework_id;
  unsigned int j_id = this->_job_id;

  Slave& slave = allSlaves[s_id];
  Framework& framework = allFrameworks[f_id];
  Task& task = allTasks[t_id];

  if (DEBUG) {
    cout << "Time is " << sim.get_clock() << " finished_task "
         << t_id << endl;
  }
  release_resources(sim, &slave, task.used_resources);
  framework.current_used -= task.used_resources;
  slave.curr_tasks.erase(t_id);

  for (deque<size_t>& task_list : framework.task_lists) {
    if (task_list.size() > 0 && task_list[0] == t_id) {
      assert(allTasks[task_list[0]].being_run);
      task_list.pop_front();
      break;
    }
  }

  jobs_to_tasks[j_id].first -= 1;
  if (jobs_to_tasks[j_id].first == 0) {
    unsigned int start = jobs_to_tasks[j_id].second;
    jobs_to_tasks[j_id].second = this->get_time() - start;
  }
}

void run_auction(const Event<MesosSimulation> &e) {
  // Collect:
  // - bids
  unordered_map<FrameworkID, vector<vector<Bid> > > all_bids;

  // - free resources
  unordered_map<SlaveID, Resources> resources;
#if 0
  for (const Slave& slave : allSlaves) {
    SlaveID slave_id = slave.id();
    resources[slave_id] = slave.free_resources;
  }
#endif
  // - reservation prices
  Resources reservation_price(1, 1, 1);
  // - minimum price increase
  double min_price_increase = 0.1;
  // - price multiplier
  double price_multiplier = 1.1;

  // Run the auction
  Auction auction(all_bids, resources, reservation_price, min_price_increase,
                  price_multiplier);
  auction.run();

  // Get the results
  const unordered_map<SlaveID, vector<Bid *> > &results = auction.results();

  // Implement the results
}
