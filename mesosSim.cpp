#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <assert.h>
#include <fstream>
#include <set>
#include <math.h>
#include <queue>
#include <stack>
#include <tr1/unordered_map>
#include <climits>
#include <algorithm>

using namespace std;
using namespace std::tr1;

// i have decided to go for a priority queue based simulation; the caveat is that code cant be directly implemented;

double Clock;

unsigned int global_event_id = 0;

typedef struct{
	unsigned int to;
	unsigned int from;
	unsigned int val;
} Msg;

Msg NULL_MSG;

class Event{
	
	friend bool operator <(const Event& e1, const Event& e2){
		return e2._etime < e1._etime;
	}		

	friend bool operator ==(const Event& e1,const Event& e2){
		return e2._etime==e1._etime;
	}
	public:
		Event(){};
		enum EvtType{send_offer, accept_offer, reject_offer, finished_task};
		Event(EvtType type, double etime, Msg msg):_type(type),_etime(etime), _msg(msg) {_id = global_event_id; global_event_id++;}
		EvtType get_type(){return _type;}
		double get_time(){return _etime;}
		int get_id(){return _id;}
		Msg get_msg(){return _msg;}
	protected:
		EvtType _type;
		double _etime;
		int _id;
		Msg _msg;
};

priority_queue<Event> FutureEventList;

typedef struct{
	double cpus;
	double mem;
	double disk;
} Resources;

typedef struct{
	unsigned int slave_id;
	unsigned int task_id;
	Resources used_resources;
	double task_time;
	bool being_run;
} Task;

class Slave{
public:
	unsigned int id;
	Resources resources;
	vector<Task> curr_tasks;
	Resources free_resources;
};

class Framework{
public:
	unsigned int id;
	vector< deque<Task> > task_lists;
	Resources current_used;
};

class Master{
public:
	vector<Framework> frameworks;
};

Slave *allSlaves;
Master master;
Framework *allFrameworks;

int num_slaves = 2;
int num_frameworks = 1;
int max_slave_id = 0;
unordered_map<unsigned int, int> map;

double t_cpus = 0.0, t_mem = 0.0, t_disk = 0.0;

// Functions handling events
void init(Slave *n);	// ok
void send_offer(Event e);
void accept_offer(Event e);
void reject_offer(Event e);
void finished_task(Event e);

int main(int argc, char *argv[]){

	Clock = 0;

	allSlaves = new Slave[num_slaves];
	allFrameworks = new Framework[num_frameworks];

	// lets initialize slave state and the event queue 
	for(int i=0;i<num_slaves;i++){
		init(&allSlaves[i]);
		map[allSlaves[i].id]=i;

		t_cpus += allSlaves[i].resources.cpus;
		t_mem += allSlaves[i].resources.mem;
		t_disk += allSlaves[i].resources.disk;
	}
	
	while(!FutureEventList.empty()){
		Event evt=FutureEventList.top();
		FutureEventList.pop();
		Clock=evt.get_time();
		
		if(evt.get_type()==Event::send_offer){
			send_offer(evt);
		}else if(evt.get_type()==Event::accept_offer){
			accept_offer(evt);
		}else if(evt.get_type()==Event::reject_offer){
			reject_offer(evt);
		}else if(evt.get_type()==Event::finished_task){
			finished_task(evt);
		}

	}

	return 0;
}

void init(Slave *n){
	n->id = max_slave_id;
	max_slave_id++;
	Resources rsrc = {4.0, 3000000000.0, 500000000000.0 };
	n->resources = rsrc;
	n->free_resources = rsrc;
}

bool task_on_slave(Slave s, Resources r){
	if(s.free_resources.cpus < r.cpus && s.free_resources.mem < r.mem && s.free_resources.disk < r.disk)
		return true;
	return false;
}

void use_resources(Slave* s, Resources r) {
	s->free_resources.cpus -= r.cpus;
	s->free_resources.mem -= r.mem;
	s->free_resources.disk -= r.disk;
}

void release_resources(Slave* s, Resources r) {
	s->free_resources.cpus += r.cpus;
	s->free_resources.mem += r.mem;
	s->free_resources.disk += r.disk;
}

int curr_rr_offer = 0;
vector<Task> curr_schedules;
unsigned int num_offers = 0;
bool make_offers = true;

void send_offer(Event e){
	Framework* f = &allFrameworks[curr_rr_offer];
	for(int i = 0; i < f->task_lists.size(); i++) {
		if(f->task_lists[i].size() == 0 || f->task_lists[i][0].being_run)
			continue;
		for(int j = 0; j < num_slaves; j++) {
			if(task_on_slave(allSlaves[j], f->task_lists[i][0].used_resources)){
				Task todo_task = f->task_lists[i][0];
				todo_task.slave_id = allSlaves[j].id;
				curr_schedules.push_back(todo_task);
				use_resources(&allSlaves[j], todo_task.used_resources);
				f->task_lists[i][0].being_run = true;
			}
		}
	}
	if(curr_schedules.size() > 0) {
		//Only needed if making offer to everyone, so everyone sees same view
/*		for(int i = 0; i < curr_schedules.size(); i++) {
			Task task = curr_schedules[i];
			release_resources(&allSlaves[map[task.slave_id]], task.used_resources);

		}*/
		Event evt(Event::accept_offer, e.get_time(), NULL_MSG);
		FutureEventList.push(evt);
	}else {
		Event evt(Event::reject_offer, e.get_time(), NULL_MSG);
	        FutureEventList.push(evt);
	}
}

void accept_offer(Event e){
	for(int i = 0; i < curr_schedules.size(); i++) {
		Task task = curr_schedules[i];
		Slave * s = &allSlaves[map[task.slave_id]];

		//Only needed if making offer to everyone, so everyone sees same view
		//use_resources(s, task.used_resources);
		s->curr_tasks.push_back(task);
		Event evt(Event::finished_task, e.get_time() + task.task_time, {task.slave_id, curr_rr_offer, task.task_id});
		FutureEventList.push(evt);

	}
	curr_schedules.clear();
	curr_rr_offer++;
	num_offers++;
	if(num_offers < num_frameworks){
		Event evt(Event::send_offer, e.get_time(), NULL_MSG);
		FutureEventList.push(evt);
	}else {
		num_offers = 0;
	}

}

void reject_offer(Event e){
	assert(curr_schedules.size()==0);
	curr_rr_offer++;
	num_offers++;
	if(num_offers < num_frameworks){
		Event evt(Event::send_offer, e.get_time(), NULL_MSG);
		FutureEventList.push(evt);
	}else {
		num_offers = 0;
	}

}

void finished_task(Event e){
	unsigned int s_id = e.get_msg().to;
	unsigned int t_id = e.get_msg().val;
	unsigned int f_id = e.get_msg().from;
	Slave * s = &allSlaves[map[s_id]];
	int i = 0;
	for(; i<s->curr_tasks.size(); i++){
		if(s->curr_tasks[i].task_id == t_id) {
			release_resources(s, s->curr_tasks[i].used_resources);
			break;
		}
	}
	s->curr_tasks.erase(s->curr_tasks.begin() + i);
	Framework* f = &allFrameworks[f_id];
	for(int i = 0; i < f->task_lists.size(); i++) {
		if(f->task_lists[i][0].task_id == t_id){
			f->task_lists[i].pop_front();
			break;
		}
	}


	Event evt(Event::send_offer, e.get_time(), NULL_MSG);
	FutureEventList.push(evt);

}
