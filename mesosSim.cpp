#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <assert.h>
#include <fstream>
#include <set>
#include <math.h>
#include <queue>
#include <stack>
#include <climits>
#include <algorithm>

//#ifdef _LIBCPP_VERSION
#include <unordered_map>
/*#else
#include <tr1/unordered_map>
using namespace std::tr1;
#endif*/

#include "shared.hpp"
#include "auction.hpp"

using namespace std;

// i have decided to go for a priority queue based simulation; the caveat is that code cant be directly implemented;

//#define RR
#define DRF
#define DEBUG 0

double Clock;

unsigned int global_event_id = 0;

struct Msg {
	unsigned int to;
	unsigned int from;
	unsigned int val;
	unsigned int val2;
};

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
		Event(EvtType type, double etime, const Msg& msg):_type(type),_etime(etime), _msg(msg) {_id = global_event_id; global_event_id++;}
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
	unsigned int slave_id;
	unsigned int task_id;
	unsigned int job_id;
	Resources used_resources;
	double task_time;
	double start_time;
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

int num_slaves = 10;
int num_frameworks = 397;
int max_slave_id = 0;
int max_job_id = 0;
unordered_map<unsigned int, int> slave_id_to_index;
unordered_map<unsigned int, pair<int, unsigned int> > jobs_to_tasks;
unordered_map<unsigned int, int > jobs_to_num_tasks;

unordered_map<unsigned int, bool> offered_framework_ids;

double total_cpus = 0.0, total_mem = 0.0, total_disk = 0.0;
double total_cpus_used = 0.0, total_mem_used = 0.0, total_disk_used = 0.0;

// Functions handling events
void init(Slave *n);	// ok
void process_offer(Event e);
void finished_task(Event e);

void rand_workload(){
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

				t.used_resources.cpus = static_cast<double>(rand()%4);
				t.used_resources.mem = static_cast<double>(rand()%5);
				t.used_resources.disk = 0;
        
				t.being_run = false;
				t.task_time = (double) (rand()%10);
				q.push_back(t);
			}
			f->task_lists.push_back(q);
		}

	}


}

vector<double> split(string str, char delim) {
	vector<double> split_v;
	int i = 0;
	string buf = "";
	while (i < str.length()){
		if (str[i] != delim)
			buf += str[i];
		else {
			split_v.push_back(atof(buf.c_str()));
			buf = "";
		}
		i++;
	}
	if (!buf.empty())
		split_v.push_back(atof(buf.c_str()));
	return split_v;
}

bool intersect(double s1, double e1, double s2, double e2){
	if (s1 > e2 || s2 > e1) {
		return false;
	}
	return true;
}

void trace_workload(){
	for(int i = 0; i< num_frameworks; i++){
		Framework* f = &allFrameworks[i]; 
		f->id = i;
		f->current_used = {0, 0, 0};
		offered_framework_ids[i] = false;
	}

	unsigned int assigned_t_id = 0;
	string line;
	ifstream trace ("task_times_converted.txt");
	if(trace.is_open()){
		cout << " IN " << endl;
		int job_vector_index = 0;
		double last_j_id = 0;
		while( getline(trace, line)) {
			// 0: job_id 1: task_index 2: start_time 3: end_time 4: framework_id 5: scheduling_class
			// 6: priority 7: cpu 8: ram 9: disk
			vector<double> split_v = split(line, ' ');
			if(split_v[0] != last_j_id) {
				max_job_id++;
			}

			Task t;
			t.slave_id = 0; //Set simply as default
			t.task_id = assigned_t_id;
			t.job_id = max_job_id;
			assigned_t_id++;


			t.used_resources = { split_v[7], split_v[8], split_v[9]};
			t.being_run = false;
			t.task_time = split_v[3] - split_v[2];
			t.start_time = split_v[2];
			if (t.task_time <= 0) {
				cout << "ERROR IN PROCESSING TASK TIME! " << t.job_id << " " << t.start_time << " " << split_v[3] << endl;
				exit(EXIT_FAILURE);
			}
			
			Framework* f = &allFrameworks[ (int) split_v[4] ];


			if ( split_v[0] != last_j_id) {
				last_j_id = split_v[0];
				job_vector_index = f->task_lists.size();
				deque<Task> q;
				q.push_back(t);
				f->task_lists.push_back(q);
				jobs_to_tasks[t.job_id] = make_pair(1, t.start_time);
				jobs_to_num_tasks[t.job_id] = 1;

			}else {
				jobs_to_tasks[t.job_id].first += 1;
				jobs_to_num_tasks[t.job_id] += 1;

				if (t.start_time < jobs_to_tasks[t.job_id].second)
					jobs_to_tasks[t.job_id].second = t.start_time;
				int i;
				for(i = job_vector_index; i < f->task_lists.size(); i++) {
					bool has_intersection = false;
					for( int j = 0; j < f->task_lists[i].size(); j++) {
						Task cmp_task = f->task_lists[i][j];
						has_intersection = intersect(t.start_time, t.start_time + t.task_time, cmp_task.start_time, cmp_task.start_time + cmp_task.task_time);
						if(has_intersection)
							break;
					}
					if( !has_intersection) {
						if(DEBUG) cout << "No intersections! " << line << endl;
						int j;
						for( j = 0; j < f->task_lists[i].size(); j++){
							if (t.start_time + t.task_time < f->task_lists[i][j].start_time){
								f->task_lists[i].insert(f->task_lists[i].begin()+j, t);
								break;
							}
						}
						if(j == f->task_lists[i].size()){
							f->task_lists[i].push_back(t);
						}
						break;
					}
				}
				if(i == f->task_lists.size()) {
					deque<Task> q;
					q.push_back(t);
					f->task_lists.push_back(q);
				}	
			}
		}
		trace.close();
	}
}

int main(int argc, char *argv[]){

	Clock = 0;

	allSlaves = new Slave[num_slaves];
	allFrameworks = new Framework[num_frameworks];

	// lets initialize slave state and the event queue 
	for(int i=0;i<num_slaves;i++){
		init(&allSlaves[i]);
		slave_id_to_index[allSlaves[i].id]=i;

		total_cpus += allSlaves[i].resources.cpus;
		total_mem += allSlaves[i].resources.mem;
		total_disk += allSlaves[i].resources.disk;
	}

	//rand_workload();
	trace_workload();
	cout << "Done generating workload" << endl;

	//Spot check
	int sum_size = 0;
	for(int i = 0; i < num_frameworks; i++) {
		for (int j = 0; j < allFrameworks[i].task_lists.size(); j++){
			if ( allFrameworks[i].task_lists[j].size() > 1 ){
				for(int k = 1; k < allFrameworks[i].task_lists[j].size(); k++){
					Task t1 = allFrameworks[i].task_lists[j][k-1];
					Task t2 = allFrameworks[i].task_lists[j][k];
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

	Event evt(Event::offer, 5637000000, NULL_MSG);
	FutureEventList.push(evt);

	for(int j=0;j<num_slaves;j++){
		if(DEBUG) cout << "Slave " << (&allSlaves[j])->id << " has total cpus: " << (&allSlaves[j])->resources.cpus << " mem: " << (&allSlaves[j])->resources.mem << endl;
	}

	while(!FutureEventList.empty()){
		if(DEBUG) cout << endl;

		Event evt=FutureEventList.top();
		FutureEventList.pop();
		Clock=evt.get_time();
		
		if (Clock > 2506180437563)
			break;

		if(evt.get_type()==Event::offer){
			process_offer(evt);
		}else if(evt.get_type()==Event::finished_task){
			finished_task(evt);
		}
/*		for(int j = 0; j < num_slaves; j++) {
			if(DEBUG) cout << "Curr slave " << (&allSlaves[j])->id << " cpu: " << (&allSlaves[j])->free_resources.cpus << " mem: " << (&allSlaves[j])->free_resources.mem << endl;
		}
		for(int j = 0; j < num_frameworks; j++) {
			Framework f = allFrameworks[j];
			double cpu_share = f.current_used.cpus / total_cpus;
			double mem_share = f.current_used.mem / total_mem;
			double disk_share = f.current_used.disk / total_disk;
			double dominant_share = max(cpu_share, max(mem_share, disk_share));
			if(DEBUG) cout << "Framework " << (&allFrameworks[j])->id << " is using cpus: " << (&allFrameworks[j])->current_used.cpus << " mem: " << (&allFrameworks[j])->current_used.mem << " Dominant share is " << dominant_share << endl;
		}*/
	}

	//METRICS: Completion time
	unsigned int sum = 0;
	cout << "#JOB COMPLETION TIMES" << endl;
	for( auto it = jobs_to_tasks.begin(); it != jobs_to_tasks.end(); ++it){
		cout << jobs_to_num_tasks[it->first] << " " << it->second.second << endl;
		//sum += it->second.second;
	}
	//if(DEBUG) cout << "Average job completition time is " << float(sum)/float(jobs_to_tasks.size()) << endl;

	return 0; 
}

void init(Slave *n){
	n->id = max_slave_id;
	max_slave_id++;
	Resources rsrc = {1, 1, 1 };
//	Resources rsrc = {4.0, 5, 1 };
	n->resources = rsrc;
	n->free_resources = rsrc;
}

bool task_on_slave(Slave* s, Resources r){
	if(s->free_resources.cpus >= r.cpus && s->free_resources.mem >= r.mem && s->free_resources.disk >= r.disk) {
		return true;
	}
	return false;
}

void use_resources(Slave* s, Resources r) {
	s->free_resources.cpus -= r.cpus;
	s->free_resources.mem -= r.mem;
	s->free_resources.disk -= r.disk;
	total_cpus_used += r.cpus;
	total_mem_used += r.mem;
	total_disk_used += r.disk;
	if(DEBUG) cout << "Using Slave " << s->id << ": " << s->free_resources.cpus << " " << s->free_resources.mem << endl;
}

void release_resources(Slave* s, Resources r) {
	s->free_resources.cpus += r.cpus;
	s->free_resources.mem += r.mem;
	s->free_resources.disk += r.disk;
	total_cpus_used -= r.cpus;
	total_mem_used -= r.mem;
	total_disk_used -= r.disk;

	if(DEBUG) cout << "Freeing Slave " << s->id << ": " << s->free_resources.cpus << " " << s->free_resources.mem << endl;

}

unsigned int curr_framework_offer = 0;
unsigned int num_offers = 0;

void round_robin() {
	curr_framework_offer++;
	curr_framework_offer %= num_frameworks;
}

void drf() {
	double max_share = 1.0;
	double next_id  = 0;
	for(int i = 0; i<num_frameworks; i++) {
		if(!offered_framework_ids[i]){
			Framework* f = &allFrameworks[i];
			double cpu_share = f->current_used.cpus / total_cpus;
			double mem_share = f->current_used.mem / total_mem;
			double disk_share = f->current_used.disk / total_disk;
			double dominant_share = max(cpu_share, max(mem_share, disk_share));
			if(DEBUG) cout << "DRF computes dominant share for framework " << i << " is " << dominant_share << endl;
			if (dominant_share < max_share) {
				max_share = dominant_share;
				next_id = i;
			}
		}
	}
	if(DEBUG) cout << "DRF is offering next to framework " << next_id << endl;
	curr_framework_offer = next_id;
	
}

void process_offer(Event e){
	if(DEBUG) cout <<"Time is " << Clock << " process_offer function" << endl;

#ifdef RR
	round_robin();
#endif
#ifdef DRF
	drf();
#endif

	Framework* f = &allFrameworks[curr_framework_offer];
	if(DEBUG) cout << "Framework " << curr_framework_offer << endl;
	offered_framework_ids[curr_framework_offer] = true;

	vector<Task> curr_schedules;

	for(int i = 0; i < f->task_lists.size(); i++) {
		if(f->task_lists[i].size() == 0 || f->task_lists[i][0].being_run || f->task_lists[i][0].start_time > e.get_time()){
			if(DEBUG) cout << "No task to currently run in thread " << i << endl;
			continue;
		}
		if(DEBUG) cout << "Task id=" << f->task_lists[i][0].task_id << " : cpu " << f->task_lists[i][0].used_resources.cpus << " mem " << f->task_lists[i][0].used_resources.mem << " being_run " << f->task_lists[i][0].being_run << endl;
		for(int j = 0; j < num_slaves; j++) {
			if(task_on_slave(&allSlaves[j], f->task_lists[i][0].used_resources)){
				if(DEBUG) cout << "Should schedule" << endl;
				Task todo_task = f->task_lists[i][0];
				todo_task.slave_id = allSlaves[j].id;
				curr_schedules.push_back(todo_task);
				use_resources(&allSlaves[j], todo_task.used_resources);
				if(DEBUG) cout << "Slave scheduled: id=" << (&allSlaves[j])->id << " cpu: " << (&allSlaves[j])->free_resources.cpus << " mem: " << (&allSlaves[j])->free_resources.mem << endl;
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
			Slave * s = &allSlaves[slave_id_to_index[task.slave_id]];
	
			//Only needed if making offer to everyone, so everyone sees same view
			//use_resources(s, task.used_resources);
			s->curr_tasks.push_back(task);
			Msg msg = {task.slave_id, curr_framework_offer, task.task_id, task.job_id};
			Event evt(Event::finished_task, e.get_time() + task.task_time, msg);
			FutureEventList.push(evt);

		}
		curr_schedules.clear();

	}
	num_offers++;
	if(num_offers < num_frameworks){
		Event evt(Event::offer, e.get_time(), NULL_MSG);
		FutureEventList.push(evt);
	}else {
		for(int i = 0; i < num_frameworks; i++) {
			offered_framework_ids[i] = false;
		}
		Event evt(Event::offer, e.get_time()+60000000, NULL_MSG);
		FutureEventList.push(evt);

		//cout << "Offers made at time " << << endl;
		//Log utilization
		cout << Clock/1000000 << " " << total_cpus_used << " " << total_cpus << " " << total_mem_used << " " << total_mem << " " << total_disk_used << " " << total_disk << endl;

		num_offers = 0;
	}

}

void finished_task(Event e){
	unsigned int s_id = e.get_msg().to;
	unsigned int t_id = e.get_msg().val;
	unsigned int f_id = e.get_msg().from;
	unsigned int j_id = e.get_msg().val2;
	Slave * s = &allSlaves[slave_id_to_index[s_id]];
	Framework* f = &allFrameworks[f_id];

	int i = 0;
	for(; i<s->curr_tasks.size(); i++){
		if(s->curr_tasks[i].task_id == t_id) {
			if(DEBUG) cout << "Time is " << Clock << " finished_task " << s->curr_tasks[i].task_id << endl;
			release_resources(s, s->curr_tasks[i].used_resources);
			f->current_used.cpus -= s->curr_tasks[i].used_resources.cpus;
			f->current_used.mem -= s->curr_tasks[i].used_resources.mem;
			f->current_used.disk -= s->curr_tasks[i].used_resources.disk;

			break;
		}
	}
	s->curr_tasks.erase(s->curr_tasks.begin() + i);
	for(int j = 0; j < f->task_lists.size(); j++) {
		if(f->task_lists[j].size() > 0 && f->task_lists[j][0].task_id == t_id){
			assert( f->task_lists[j][0].being_run );
			f->task_lists[j].pop_front();
			break;
		}
	}
	jobs_to_tasks[j_id].first -= 1;
	if(jobs_to_tasks[j_id].first==0){
		unsigned int start = jobs_to_tasks[j_id].second;
		jobs_to_tasks[j_id].second = e.get_time() - start;
	}

}
