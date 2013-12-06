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

#include "shared.hpp"
#include "auction.hpp"

using namespace std;

// i have decided to go for a priority queue based simulation; the caveat is
// that code cant be directly implemented;

//#define RR
#define DRF
#define DEBUG 0

double Clock;

class Event {
  friend bool operator<(const Event &e1, const Event &e2) {
    return e2._etime < e1._etime;
  }

  friend bool operator==(const Event &e1, const Event &e2) {
    return e2._etime == e1._etime;
  }

 public:
  Event() {}
  virtual ~Event() {}
  Event(double etime) : _etime(etime) {}
  double get_time() { return _etime; }

  virtual void run() = 0;

 protected:
  double _etime;
};

class OfferEvent : public Event {
 public:
  OfferEvent(double etime) : Event(etime) {}
  virtual void run();
};

class FinishedTaskEvent : public Event {
 public:
  FinishedTaskEvent(double etime, unsigned int slave_id,
                    unsigned int framework_id, unsigned int task_id,
                    unsigned int job_id)
      : Event(etime),
        _slave_id(slave_id),
        _framework_id(framework_id),
        _task_id(task_id),
        _job_id(job_id) {}
  virtual void run();

 private:
  unsigned int _slave_id;
  unsigned int _framework_id;
  unsigned int _task_id;
  unsigned int _job_id;
};

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
  Slave() {
    Resources rsrc = { 1, 1, 1 };
    //	Resources rsrc = {4.0, 5, 1 };
    resources = rsrc;
    free_resources = rsrc;
  }
  Resources resources;
  unordered_set<size_t> curr_tasks;
  Resources free_resources;
};

class Framework : public Indexable {
 public:
  vector<deque<size_t>> task_lists;
  Resources current_used;
};

class Master {
 public:
  vector<Framework> frameworks;
};

Indexer<Slave> allSlaves;
Indexer<Framework> allFrameworks;
Indexer<Task> allTasks;
Master master;

priority_queue<Event*, vector<Event*>, pless<Event>> FutureEventList;

int num_slaves = 10;
int num_frameworks = 397;
int max_job_id = 0;

unordered_map<unsigned int, pair<int, unsigned int> > jobs_to_tasks;
unordered_map<unsigned int, int> jobs_to_num_tasks;

Resources total_resources, used_resources;

// Functions handling events
void run_auction(const Event &e);

void rand_workload() {
  // This simply generates random tasks for debugging purposes
  srand(0);
  for (int i = 0; i < num_frameworks; i++) {
    Framework& framework = allFrameworks.add(); 
    for (int j = 0; j < 2; j++) {
      deque<size_t> q;
      for (int k = 0; k < 2; k++) {
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

void trace_workload() {
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

int main(int argc, char *argv[]) {

  Clock = 0;

  // lets initialize slave state and the event queue
  for (int i = 0; i < num_slaves; i++) {
    allSlaves.add();
    total_resources += allSlaves[i].resources;
  }

  //rand_workload();
  trace_workload();
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

  FutureEventList.push(new OfferEvent(5637000000));

  if (DEBUG) {
    for (const auto& slave : allSlaves) {
      cout << "Slave " << slave.id()
           << " has total cpus: " << slave.resources.cpus
           << " mem: " << slave.resources.mem << endl;
    }
  }

  while (!FutureEventList.empty()) {
    if (DEBUG) cout << endl;

    Event* evt = FutureEventList.top();
    FutureEventList.pop();
    Clock = evt->get_time();

    if (Clock > 2506181000000) break;

    evt->run();
    delete evt;
  }

  //METRICS: Completion time
  unsigned int sum = 0;
  cout << "#JOB COMPLETION TIMES" << endl;
  for (auto it = jobs_to_tasks.begin(); it != jobs_to_tasks.end(); ++it) {
    cout << jobs_to_num_tasks[it->first] << " " << it->second.second << endl;
    //sum += it->second.second;
  }
  //if(DEBUG) cout << "Average job completition time is " <<
  //float(sum)/float(jobs_to_tasks.size()) << endl;

  return 0;
}

void use_resources(Slave *s, Resources r) {
  s->free_resources -= r;
  total_resources += r;
  if (DEBUG)
    cout << "Using Slave " << s->id() << ": " << s->free_resources.cpus << " "
         << s->free_resources.mem << endl;
}

void release_resources(Slave *s, Resources r) {
  s->free_resources += r; 
  used_resources -= r;
  if (DEBUG)
    cout << "Freeing Slave " << s->id() << ": " << s->free_resources.cpus << " "
         << s->free_resources.mem << endl;

}

unsigned int curr_framework_offer = 0;

void round_robin() {
  curr_framework_offer++;
  curr_framework_offer %= num_frameworks;
}

void drf() {
  double max_share = 1.0;
  double next_id = 0;
  for (int i = 0; i < allFrameworks.size(); i++) {
    const Framework& f = allFrameworks[i];
    double cpu_share = f.current_used.cpus / total_resources.cpus;
    double mem_share = f.current_used.mem / total_resources.mem;
    double disk_share = f.current_used.disk / total_resources.disk;
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

void OfferEvent::run() {
  if (DEBUG) cout << "Time is " << Clock << " process_offer function" << endl;

#ifdef RR
  round_robin();
#endif
#ifdef DRF
  drf();
#endif

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
        use_resources(&slave, todo_task.used_resources);

        todo_task.being_run = true;
        f.current_used += todo_task.used_resources;

        slave.curr_tasks.insert(todo_task.id());
        FutureEventList.push(
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

  FutureEventList.push(new OfferEvent(this->get_time() + 60000000));

  //cout << "Offers made at time " << << endl;
  //Log utilization
  cout << Clock / 1000000 << " " << used_resources.cpus << " " << total_resources.cpus
       << " " << used_resources.mem << " " << total_resources.mem 
       << " " << used_resources.disk << " " << total_resources.disk << endl;
}

void FinishedTaskEvent::run() {
  unsigned int s_id = this->_slave_id;
  unsigned int t_id = this->_task_id;
  unsigned int f_id = this->_framework_id;
  unsigned int j_id = this->_job_id;

  Slave& slave = allSlaves[s_id];
  Framework& framework = allFrameworks[f_id];
  Task& task = allTasks[t_id];

  if (DEBUG) {
    cout << "Time is " << Clock << " finished_task "
         << t_id << endl;
  }
  release_resources(&slave, task.used_resources);
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

void run_auction(const Event &e) {
  // Collect:
  // - bids
  unordered_map<FrameworkID, vector<vector<Bid> > > all_bids;

  // - free resources
  unordered_map<SlaveID, Resources> resources;
  for (const Slave& slave : allSlaves) {
    SlaveID slave_id = slave.id();
    resources[slave_id] = slave.free_resources;
  }
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
