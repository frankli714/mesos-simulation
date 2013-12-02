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

//#define RR
#define DRF

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
		enum EvtType{offer, finished_task};
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
int num_frameworks = 3;
int max_slave_id = 0;
unordered_map<unsigned int, int> map;

double total_cpus = 0.0, total_mem = 0.0, total_disk = 0.0;

// Functions handling events
void init(Slave *n);	// ok
void process_offer(Event e);
void finished_task(Event e);

int main(int argc, char *argv[]){

	Clock = 0;

	allSlaves = new Slave[num_slaves];
	allFrameworks = new Framework[num_frameworks];

	// lets initialize slave state and the event queue 
	for(int i=0;i<num_slaves;i++){
		init(&allSlaves[i]);
		map[allSlaves[i].id]=i;

		total_cpus += allSlaves[i].resources.cpus;
		total_mem += allSlaves[i].resources.mem;
		total_disk += allSlaves[i].resources.disk;
	}

	unsigned int assigned_t_id = 0;
	// This simply generates random tasks for debugging purposes
	srand(0);
	for(int i = 0; i < num_frameworks; i++) {
		Framework* f = &allFrameworks[i];
		f->id = i;
		Resources r;
		r.cpus = 0;
		r.mem = 0;
		r.disk = 0;
		f->current_used = r;
		for(int j = 0; j < 2; j++) {
			deque<Task> q;
			for(int k = 0; k<2; k++) {
				Task t;
				t.task_id = assigned_t_id;
				assigned_t_id++;
				t.used_resources = {(double) (rand()%4), (double) (rand()%5), 0};
				t.being_run = false;
				t.task_time = (double) (rand()%10);
				q.push_back(t);
			}
			f->task_lists.push_back(q);
		}

	}

	Event evt(Event::offer, 0, NULL_MSG);
	FutureEventList.push(evt);

	for(int j=0;j<num_slaves;j++){
		cout << "Slave " << (&allSlaves[j])->id << " has total cpus: " << (&allSlaves[j])->resources.cpus << " mem: " << (&allSlaves[j])->resources.mem << endl;
	}

	while(!FutureEventList.empty()){
		cout << endl;

		Event evt=FutureEventList.top();
		FutureEventList.pop();
		Clock=evt.get_time();

		if(evt.get_type()==Event::offer){
			process_offer(evt);
		}else if(evt.get_type()==Event::finished_task){
			finished_task(evt);
		}
		for(int j = 0; j < num_slaves; j++) {
			cout << "Curr slave " << (&allSlaves[j])->id << " cpu: " << (&allSlaves[j])->free_resources.cpus << " mem: " << (&allSlaves[j])->free_resources.mem << endl;
		}
		for(int j = 0; j < num_frameworks; j++) {
			Framework f = allFrameworks[j];
			double cpu_share = f.current_used.cpus / total_cpus;
			double mem_share = f.current_used.mem / total_mem;
			double disk_share = f.current_used.disk / total_disk;
			double dominant_share = max(cpu_share, max(mem_share, disk_share));
			cout << "Framework " << (&allFrameworks[j])->id << " is using cpus: " << (&allFrameworks[j])->current_used.cpus << " mem: " << (&allFrameworks[j])->current_used.mem << " Dominant share is " << dominant_share << endl;
		}
	}

	return 0;
}

void init(Slave *n){
	n->id = max_slave_id;
	max_slave_id++;
//	Resources rsrc = {4.0, 3000000000.0, 500000000000.0 };
	Resources rsrc = {4.0, 5, 1 };
	n->resources = rsrc;
	n->free_resources = rsrc;
}

bool task_on_slave(Slave s, Resources r){
	if(s.free_resources.cpus >= r.cpus && s.free_resources.mem >= r.mem && s.free_resources.disk >= r.disk) {
		return true;
	}
	return false;
}

void use_resources(Slave* s, Resources r) {
	s->free_resources.cpus -= r.cpus;
	s->free_resources.mem -= r.mem;
	s->free_resources.disk -= r.disk;
	cout << "Using Slave " << s->id << ": " << s->free_resources.cpus << " " << s->free_resources.mem << endl;
}

void release_resources(Slave* s, Resources r) {
	s->free_resources.cpus += r.cpus;
	s->free_resources.mem += r.mem;
	s->free_resources.disk += r.disk;
	cout << "Freeing Slave " << s->id << ": " << s->free_resources.cpus << " " << s->free_resources.mem << endl;

}

int curr_framework_offer = 0;
unsigned int num_offers = 0;
bool make_offers = true;
vector<unsigned int> offered_framework_ids;

void round_robin() {
	curr_framework_offer++;
	curr_framework_offer %= num_frameworks;
}

void drf() {
	double max_share = 1.0;
	double next_id  = 0;
	for(int i = 0; i<num_frameworks; i++) {
		if(find(offered_framework_ids.begin(), offered_framework_ids.end(), i) == offered_framework_ids.end()){
			Framework f = allFrameworks[i];
			double cpu_share = f.current_used.cpus / total_cpus;
			double mem_share = f.current_used.mem / total_mem;
			double disk_share = f.current_used.disk / total_disk;
			double dominant_share = max(cpu_share, max(mem_share, disk_share));
			cout << "DRF computes dominant share for framework " << i << " is " << dominant_share << endl;
			if (dominant_share < max_share) {
				max_share = dominant_share;
				next_id = i;
			}
		}
	}
	cout << "DRF is offering next to framework " << next_id << endl;
	curr_framework_offer = next_id;
	
}

void process_offer(Event e){
	make_offers = true;
	cout <<"Time is " << Clock << " process_offer function" << endl;

#ifdef RR
	round_robin();
#endif
#ifdef DRF
	drf();
#endif

	Framework* f = &allFrameworks[curr_framework_offer];
	cout << "Framework " << curr_framework_offer << endl;
	offered_framework_ids.push_back(curr_framework_offer);

	vector<Task> curr_schedules;

	for(int i = 0; i < f->task_lists.size(); i++) {
		if(f->task_lists[i].size() == 0 || f->task_lists[i][0].being_run){
			cout << "No task to currently run in thread " << i << endl;
			continue;
		}
		cout << "Task id=" << f->task_lists[i][0].task_id << " : cpu " << f->task_lists[i][0].used_resources.cpus << " mem " << f->task_lists[i][0].used_resources.mem << " being_run " << f->task_lists[i][0].being_run << endl;
		for(int j = 0; j < num_slaves; j++) {
			if(task_on_slave(allSlaves[j], f->task_lists[i][0].used_resources)){
				cout << "Should schedule" << endl;
				Task todo_task = f->task_lists[i][0];
				todo_task.slave_id = allSlaves[j].id;
				curr_schedules.push_back(todo_task);
				use_resources(&allSlaves[j], todo_task.used_resources);
				cout << "Slave scheduled: id=" << (&allSlaves[j])->id << " cpu: " << (&allSlaves[j])->free_resources.cpus << " mem: " << (&allSlaves[j])->free_resources.mem << endl;
				f->task_lists[i][0].being_run = true;
				f->current_used.cpus += todo_task.used_resources.cpus;
				f->current_used.mem += todo_task.used_resources.mem;
				f->current_used.disk += todo_task.used_resources.disk;

				break;
			}
		}
	}
	if(curr_schedules.size() > 0) {
		//Only needed if making offer to everyone, so everyone sees same view
/*		for(int i = 0; i < curr_schedules.size(); i++) {
			Task task = curr_schedules[i];
			release_resources(&allSlaves[map[task.slave_id]], task.used_resources);

		}*/
		for(int i = 0; i < curr_schedules.size(); i++) {
			Task task = curr_schedules[i];
			Slave * s = &allSlaves[map[task.slave_id]];
	
			//Only needed if making offer to everyone, so everyone sees same view
			//use_resources(s, task.used_resources);
			s->curr_tasks.push_back(task);
			Event evt(Event::finished_task, e.get_time() + task.task_time, {task.slave_id, curr_framework_offer, task.task_id});
			FutureEventList.push(evt);

		}

	}
	num_offers++;
	if(num_offers < num_frameworks){
		Event evt(Event::offer, e.get_time(), NULL_MSG);
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
	Framework* f = &allFrameworks[f_id];

	int i = 0;
	for(; i<s->curr_tasks.size(); i++){
		if(s->curr_tasks[i].task_id == t_id) {
			cout << "Time is " << Clock << " finished_task " << s->curr_tasks[i].task_id << endl;
			release_resources(s, s->curr_tasks[i].used_resources);
			f->current_used.cpus -= s->curr_tasks[i].used_resources.cpus;
			f->current_used.mem -= s->curr_tasks[i].used_resources.mem;
			f->current_used.disk -= s->curr_tasks[i].used_resources.disk;

			break;
		}
	}
	s->curr_tasks.erase(s->curr_tasks.begin() + i);
	for(int i = 0; i < f->task_lists.size(); i++) {
		if(f->task_lists[i][0].task_id == t_id){
			assert(f->task_lists[i][0].being_run);
			f->task_lists[i].pop_front();
			break;
		}
	}

	if(make_offers){
		offered_framework_ids.clear();
		// Add 0.0001 so that the offer happens after all finished_events that happen at the same time. 
		// This ensure a single offer after all simultaneous finish_events
		Event evt(Event::offer, e.get_time()+0.0001, NULL_MSG);
		FutureEventList.push(evt);
		make_offers = false;
	}

}
