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

struct Msg{
	unsigned int from;
	unsigned int to;
	unsigned int value;
	string msg;
};

class Event{
	
	friend bool operator <(const Event& e1, const Event& e2){
		return e2._etime < e1._etime;
	}		

	friend bool operator ==(const Event& e1,const Event& e2){
		return e2._etime==e1._etime;
	}
	public:
		Event(){};
		// TODO: Specify event types
		enum EvtType{register_framework, unregister_framework, send_offer, accept_offer, reject_offer, start_task, finish_task, finish_job};
		Event(EvtType type, double etime, int id, struct Msg message):_type(type),_etime(etime),_id(id),_message(message){}
		EvtType get_type(){return _type;}
		double get_time(){return _etime;}
		int get_id(){return _id;}
		struct Msg get_message(){return _message;}
	protected:
		EvtType _type;
		double _etime;
		int _id;
		struct Msg _message;
};

priority_queue<Event> FutureEventList;

struct Resources{
	double cpus;
	double mem;
	double disk;
};

struct Task{
	unsigned int task_id;
	Resources used_resources;
	double task_time;
};

struct Job{
	unsigned int id;
	vector<Task> tasks;
};

struct Slave{
	unsigned int id;
	Resources resources;
	int alive;
	vector<Task> curr_tasks;
	Resources free_resources;
};

struct Framework{
	unsigned int id;
	vector<Job> job_list;
};

struct Master{
	Resources total_resources;
	Resources total_free_resources;	
	vector<Framework> frameworks;
};

struct Offer{
	vector<Slave> free_slave_list;
};

struct Offer_Acceptance{
	vector< vector<Task> > task_assignments;
};



struct Slave *allSlaves;
struct Master master;

int num_slaves = 2;
int max_slave_id = 0;
set<unsigned int> idlist,idlist_alive;
unordered_map<unsigned int, int> map;

// Functions handling events
void init(struct Slave *n);	// ok
void register_framework(Event e);
void unregister_framework(Event e);
void send_offer(Event e);
void accept_offer(Event e);
void reject_offer(Event e);
void start_task(Event e);
void finish_task(Event e);
void finish_job(Event e);

int main(int argc, char *argv[]){

	Clock = 0;

	allSlaves = new struct Slave[num_slaves];

	// lets initialize slave state and the event queue 
	double t_cpus = 0.0, t_mem = 0.0, t_disk = 0.0;
	for(int i=0;i<num_slaves;i++){
		init(&allSlaves[i]);
		map[allSlaves[i].id]=i;

		t_cpus += allSlaves[i].resources.cpus;
		t_mem += allSlaves[i].resources.mem;
		t_disk += allSlaves[i].resources.disk;
	}
	
	Resources master_rsrc = {t_cpus, t_mem, t_disk};
	master.total_resources = master_rsrc;
	master.total_free_resources = master_rsrc;

	cout << master.total_free_resources.cpus << " : "
		<< master.total_free_resources.mem << " : "
		<< master.total_free_resources.disk << endl;

	struct Msg null_message;
        //cout << FutureEventList.empty() << endl;
	while(!FutureEventList.empty()){
		Event evt=FutureEventList.top();
		FutureEventList.pop();
		Clock=evt.get_time();
		struct Slave * n;
		n=&allSlaves[map[evt.get_id()]];
		if(evt.get_id()!=n->id){
			cout << evt.get_id() <<  " " << n->id  << endl;
			struct Msg message=evt.get_message();
			cout << message.from << "  " << message.to << endl;
		}
		assert(evt.get_id()==n->id);
		if(n->alive==0){
			//TODO: What to do about down slaves?	
		}
		else if(evt.get_type()==Event::register_framework){
		}

	}

	while(!FutureEventList.empty()){
		FutureEventList.pop();
	}

	return 0;
}

void init(struct Slave *n){
	n->id = max_slave_id;
	max_slave_id++;
	n->alive = 1;
	Resources rsrc = {4.0, 3000000000.0, 500000000000.0 };
	n->resources = rsrc;
	n->free_resources = rsrc;
}

void register_framework(Event e){

}

void unregister_framework(Event e){

}

void send_offer(Event e){

}

void accept_offer(Event e){

}

void reject_offer(Event e){

}

void start_task(Event e){

}

void finish_task(Event e){

}

void finish_job(Event e){

}

