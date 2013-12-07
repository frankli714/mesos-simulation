#ifndef __SIMULATION_HPP__
#define __SIMULATION_HPP__

#include "shared.hpp"

template <typename Sim>
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

  virtual void run(Sim& sim) = 0;

 protected:
  double _etime;
};

template <typename T>
class Simulation {
 public:
  void add_event(Event<T>* e);
  void run();

  double get_clock() const { return Clock; }
  void set_remaining_tasks(int num_tasks) { num_remaining_tasks = num_tasks; }
  void decrement_remaining_tasks() { num_remaining_tasks--; }
 private:
  double Clock;
  int num_remaining_tasks;
  priority_queue<Event<T>*, vector<Event<T>*>, pless<Event<T>>> FutureEventList;
};

template <typename T>
void Simulation<T>::add_event(Event<T>* e) {
  FutureEventList.push(e);
}

template <typename T>
void Simulation<T>::run() {
  while (!FutureEventList.empty()) {
    if (DEBUG) cout << endl;

    Event<T>* evt = FutureEventList.top();
    FutureEventList.pop();
    Clock = evt->get_time();

    evt->run(*static_cast<T*>(this));
    delete evt;

    if (num_remaining_tasks == 0) break;
  }
}

#endif
