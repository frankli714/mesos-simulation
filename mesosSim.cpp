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
	unsigned int value2;
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
		enum EvtType{job_request, start_job, finish_job};
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

struct JobRequest{
	unsigned int job_id;
	Resources requested_resources;
	double default_job_time;
};

struct Job{
	unsigned int job_id;
	Resources used_resources;
	double job_time;
};

struct Slave{
	unsigned int id;
	Resources resources;
	int alive;
	vector<Job> curr_jobs;
	Resources free_resources;
};

struct Master{
	Resources total_resources;
	Resources total_free_resources;	
};

struct Slave *allSlaves;
struct Master master;

int num_slaves = 2;
int max_slave_id = 0;
set<unsigned int> idlist,idlist_alive;
unordered_map<unsigned int, int> map;

// Functions handling events
void init(struct Slave *n);	// ok
void job_request(struct JobRequests jr);

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
		else if(evt.get_type()==Event::job_request){
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

void job_request(struct JobRequest jr){
	double cpu_slowdown = 1.0 / min(master.total_free_resources.cpus/jr.requested_resources.cpus, 1);
	double mem_slowdown = 1.0 / min(master.total_free_resources.mem/jr.requested_resources.mem, 1);
	double disk_slowdown = 1.0 / min(master.total_free_resources.disk/jr.requested_resources.disk, 1);
	double expected_time = jr.default_job_time * cpu_slowdown * mem_slowdown * disk_slowdown;
}
