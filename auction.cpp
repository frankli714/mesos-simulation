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

struct Bid{
    double wtp;
    double time;
    unsigned int slave_id;
    unsigned int task_id;
    Resources requested_resources;
};

unsigned int num_resources = 3;

int displace(auction_state, bid){
    request = bid.requested_resources;
    for (int i = 0; i < num_resources; i++){
        cheapest[i]
    for (int i=0; i < 
