#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <vector>

#include "auction.hpp"
#include "shared.hpp"
#define RATE = 0.01

using namespace std;

// FIXME
int num_frameworks = 0;

Auction::Auction(
      const unordered_map<FrameworkID, vector<vector<Bid> > >& _all_bids,
      const unordered_map<SlaveID, Resources>& _resources,
      const Resources& _reservation_price,
      const double _min_price_increase, 
      const double _price_multiplier)
  : all_bids(_all_bids), resources(_resources),
    reservation_price(_reservation_price),
    min_price_increase(_min_price_increase), 
    price_multiplier(_price_multiplier) {}

bool Auction::displace(const Bid& new_bid,
                       vector<Bid*>& displaced_bids,
                       double& total_cost) {
  // Compute how much more resources we need to accomodate the new bid.
  SlaveID slave_id = new_bid.slave_id;
  Resources needed_resources = 
    free_resources_per_slave[slave_id].missing(new_bid.requested_resources);
  if (needed_resources.zero())
    return true;

  // Sum together the cheapest existing bids on the machine to kick out until we
  // can accomodate the new bid.
  const vector<Bid*>& bids = winning_bids_per_slave[slave_id];
  total_cost = (new_bid.requested_resources * reservation_price).sum();
  Resources freed_resources;
  for (const auto& bid : bids) {
    // Pass over your own bids.
    if (bid->framework_id == new_bid.framework_id) continue;

    // Adjust for reservation price.
    total_cost -= (bid->requested_resources * reservation_price).sum();
    total_cost += bid->current_price;

    displaced_bids.push_back(bid);
    freed_resources += bid->requested_resources;
    if (freed_resources >= needed_resources) {
      return true;
    }
  }

  // Total resources were not enough.
  return false;
}

void Auction::run() {
  // Loop continuously over all frameworks until
  // none of the frameworks in one loop want to update.
  FrameworkID framework = 0;
  for (unsigned int iterations_not_updated = 0;
       iterations_not_updated < num_frameworks;
       ++iterations_not_updated) {
    // For each framework:
    // - loop over vector of vector of Bids
    // - for each vector of Bids which doesn't have any which are winning,
    //   find the cheapest one
    
    for (auto& bids : all_bids[framework]) {
      // Determine if any of these are winning
      bool winning = any_of(bids.begin(), bids.end(),
          [](const Bid& bid) { return bid.winning; });
      if (winning) break;
       
      // If none are winning, then find the cheapest way to make one of them win
      // (cheapest = greatest difference between wtp and current_price)
      double best_profit = 0;
      Bid* best_bid;
      vector<Bid*> best_displaced_bids;
      bool found_profitable = false;
      for (auto& bid : bids) {
        vector<Bid*> displaced_bids;
        double total_cost;
        bool success = displace(bid, displaced_bids, total_cost);
        if (!success) continue;

        double profit = bid.wtp - total_cost * price_multiplier
                                - min_price_increase ;
        if (profit > best_profit) {
          best_bid = &bid;
          best_profit = profit;
          best_displaced_bids.swap(displaced_bids);
          found_profitable = true;
        }
      }

      // If cheapest way is not profitable, then give up
      if (!found_profitable) break;
      
      iterations_not_updated = 0;
      // Evict the bids which will be displaced by this new winning bid
      for (Bid* bid : best_displaced_bids) {
        bid->winning = false;
        auto& winning_bids = winning_bids_per_slave[bid->slave_id];
        for (auto it = winning_bids.begin(); it != winning_bids.end(); ++it) {
          if (*it == bid) {
            winning_bids.erase(it);
            break;
          }
        }
      }

      // Set the current price of the new bid
      best_bid->current_price = best_bid->wtp - best_profit;

      // Add this bid to the list of winning bids
      best_bid->winning = true;
      winning_bids_per_slave[best_bid->slave_id].push_back(best_bid);
      sort(winning_bids_per_slave[best_bid->slave_id].begin(),
           winning_bids_per_slave[best_bid->slave_id].end(),
           [](Bid* first, Bid* second) {
             return first->current_price < second->current_price;
           });
    }

    ++framework;
    framework %= num_frameworks;
  }
}

/*
double bids_from_tasks(unordered_map<SlaveID, Resources> free,
        vector<Task*> tasks,
        vector<vector<Bid>> bids,
        double low,
        double high,
        double budget){
    for(task t in tasks){
        if (task->being_run) break;
        task_bids = new Vector<Bid*>
        for (auto slave_resources in free){
            if (r <= slave_resources.second){
                Bid b;
                b.requested_resources = t.needed_resources;
                b.slave_id = slave_resources.first;
                b.wtp = sqrt( low*low + RATE * budget * (high - low) )
                task_bids.push_back(b)
            }
        }
        bids.push_back(task_bids);
    }
}
*/
