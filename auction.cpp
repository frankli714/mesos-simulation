#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <utility>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "auction.hpp"
#include "shared.hpp"
#define RATE = 0.01

using namespace std;

inline std::ostream& operator<<(std::ostream& o, const Bid& bid) {
  o << "{wtp=" << bid.wtp 
    << ", time=" << bid.time
    << ", fid=" << bid.framework_id
    << ", sid=" << bid.slave_id 
    << ", tid=" << bid.task_id
    << ", res=" << bid.requested_resources
    << ", win=" << bid.winning
    << ", crp=" << bid.current_price
    << "}";
  return o;
}

double value_of(const Bid& bid){
    return bid.current_price / (bid.requested_resources.weight() + 0.0000001);
}



inline std::ostream& operator<<(std::ostream& o, const Bid* bid) {
  o << *bid;
  return o;
}

Auction::Auction(
    const unordered_map<FrameworkID, vector<vector<Bid> > >& _all_bids,
    const unordered_map<SlaveID, Resources>& _resources,
    const Resources& _reservation_price, const double _min_price_increase,
    const double _price_multiplier)
    : all_bids(_all_bids),
      resources(_resources),
      reservation_price(_reservation_price),
      min_price_increase(_min_price_increase),
      price_multiplier(_price_multiplier) {}

bool Auction::displace(const Bid& new_bid, vector<Bid*>& displaced_bids,
                       double& total_cost) {
  // Start with setting the reservation price.
  total_cost = (new_bid.requested_resources * reservation_price).sum();

  // Compute how much more resources we need to accomodate the new bid.
  SlaveID slave_id = new_bid.slave_id;
  if (!(new_bid.requested_resources < resources[slave_id])) {
    VLOG(2) << "    Slave too small: " << new_bid << " will never fit in "
            << resources[slave_id];
    return false;
  }

  Resources needed_resources =
      resources[slave_id].missing(new_bid.requested_resources);
  if (needed_resources.zero()) {
    VLOG(2) << "    Already fine: "
            << new_bid << " fits in " << resources[slave_id]
            << " with cost " << total_cost;
    return true;
  }

  // Sum together the cheapest existing bids on the machine to kick out until we
  // can accomodate the new bid.
  const vector<Bid*>& bids = winning_bids_per_slave[slave_id];
  Resources freed_resources;

  VLOG(2) << "    Needs displacement: "
          << new_bid << " needs " << needed_resources  
          << " to fit in " << resources[slave_id]
          << "; starting cost " << total_cost;
  for (const auto& bid : bids) {
    // Pass over your own bids.
    if (bid->framework_id == new_bid.framework_id){
        VLOG(2) << "Passing over our own bid.";
        continue;
    }

    // Adjust for reservation price.
    total_cost -= (bid->requested_resources * reservation_price).sum();
    // Kick out this bid.
    total_cost += bid->wtp;
    displaced_bids.push_back(bid);
    freed_resources += bid->requested_resources;

    VLOG(2) << "    "
            << "Kicking out " << bid << "; freed resources=" << freed_resources
            << ", cost now " << total_cost;
    if (freed_resources >= needed_resources) {
      VLOG(2) << "    Success: " << freed_resources << " >= " <<
        needed_resources;
      return true;
    }
  }

  // Total resources were not enough despite the check above.
  LOG(FATAL) << "    Failure: " << freed_resources << " < " <<
    needed_resources;

  return false;
}

void Auction::maintain_order(size_t slave_id){
        sort(winning_bids_per_slave[slave_id].begin(),
             winning_bids_per_slave[slave_id].end(),
             [](Bid * first, Bid * second) {
          return value_of(*first) < value_of(*second);
        });
}


void Auction::run() {
  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Starting auction with " << reservation_price
            << " reservation price, " << min_price_increase
            << " min price increase, " << price_multiplier 
            << " price multiplier.";
    VLOG(1) << "Available resources:";
    vector<SlaveID> slaves;
    for (const auto& kv : resources) {
      slaves.push_back(kv.first);
    }
    sort(slaves.begin(), slaves.end());
    for (const SlaveID& slave_id : slaves) {
      VLOG(1) << "  slave " << slave_id << ": " << resources[slave_id];
    }

    for (const auto& kv : all_bids) {
      FrameworkID framework_id = kv.first;
      if (kv.second.size()) {
        VLOG(1) << "Framework " << framework_id << "'s bids:";
      }

      for (int i = 0; i < kv.second.size(); ++i) {
        const vector<Bid>& bids = kv.second[i];
        if (bids.empty()) continue;

        vector<double> wtps;
        vector<Resources> resources;
        vector<SlaveID> slave_ids;
        for (const Bid bid : bids) {
          wtps.push_back(bid.wtp);
          resources.push_back(bid.requested_resources);
          slave_ids.push_back(bid.slave_id);
        }
        VLOG(1) << "  " << i << ". wtp=[" << join(wtps, ", ") << "], " 
                << "resources=[" << join(resources, ", ") << "], " 
                << "slaves=[" << join(slave_ids, ", ") << "]";
      }
    }
  }

  // Loop continuously over all frameworks until
  // none of the frameworks in one loop want to update.
  vector<FrameworkID> frameworks;
  for (const auto& kv : all_bids) {
    frameworks.push_back(kv.first);
  }
  sort(frameworks.begin(), frameworks.end());

  while (true) {
    bool updated = false;
    for (FrameworkID framework : frameworks) {
      // For each framework:
      // - loop over vector of vector of Bids
      // - for each vector of Bids which doesn't have any which are winning,
      //   find the cheapest one
      
      if (!all_bids[framework].empty()) {
        VLOG(1) << "Framework " << framework << "'s turn:";
      }
      
      for (int i = 0; i < all_bids[framework].size(); ++i) {
        auto& bids = all_bids[framework][i];

          // Determine if any of these are winning
        bool winning = any_of(bids.begin(), bids.end(), [](const Bid & bid) {
          return bid.winning;
        });
        if (winning) {
          VLOG(1) << "  " << i << ". already winning";

          // Check a different set of bids.
          continue;
        }

        // Since none are winning, find the cheapest way to make one of them
        // win (cheapest = greatest difference between wtp and current_price)
        double best_profit = 0;
        Bid* best_bid;
        vector<Bid*> best_displaced_bids;
        vector<pair<double, vector<Bid*>>> all_displaced_bids;
        bool found_profitable = false;
        int best_bid_index = -1;

        for (int j = 0; j < bids.size(); ++j) {
          auto& bid = bids[j];

          vector<Bid*> displaced_bids;
          double total_cost;
          bool success = displace(bid, displaced_bids, total_cost);
          if (!success){
                           continue;
          }

          double profit =
              bid.wtp - total_cost * price_multiplier - min_price_increase;
              if (profit < 0){
                  all_displaced_bids.push_back(make_pair(-profit, displaced_bids));
              }

          if (profit > best_profit) {
            best_bid = &bid;
            best_profit = profit;
            best_displaced_bids.swap(displaced_bids);
            found_profitable = true;
            best_bid_index = j;
          }
        }

        // If cheapest way is not profitable, then give up
        if (!found_profitable) {

            for (pair <double, vector<Bid*>> p : all_displaced_bids){
                for (Bid* bid : p.second){
                    VLOG(2) << "Incrementing " << bid << " using gap " << p.first;
                    bid->current_price = max(bid->current_price, bid->wtp - p.first);
                    VLOG(2) << "Incremented " << bid;
                }
                if (! p.second.empty()) { 
                    this->maintain_order(p.second.front()->slave_id);
                }
            }
         
          VLOG(1) << "  " << i << ". found no profitable way";
          continue;
        }

        // Evict the bids which will be displaced by this new winning bid
        updated = true;
        VLOG(1) << "  " << i << ". "
                << "displaced with bid " << best_bid_index
                << ", slave " << best_bid->slave_id
                << ", price " << best_bid->current_price
                << " -> " << best_bid->wtp - best_profit
                << ", displaced [" << join(best_displaced_bids, ", ") << "]";
        for (Bid* bid : best_displaced_bids) {
          bid->winning = false;
          resources[bid->slave_id] += bid->requested_resources;
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
        this->maintain_order(best_bid->slave_id);
        // Subtract the amount of available resources
        resources[best_bid->slave_id] -= best_bid->requested_resources;
      }
    }

    if (!updated) break;
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
/* vim: set ts=2 sts=2 sw=2 tw=80 expandtab */
