#ifndef __AUCTION_HPP__
#define __AUCTION_HPP__

#include <unordered_map>
#include <vector>
#include <string>

#include "shared.hpp"

using namespace std;

struct Bid {
  Bid() : winning(false), current_price(0.) {}

  double wtp;
  double time;
  FrameworkID framework_id;
  SlaveID slave_id;
  TaskID task_id;
  Resources requested_resources;

  bool winning;
  double current_price;
};

class Auction {
 public:
  Auction(const unordered_map<FrameworkID, vector<vector<Bid>>>& _all_bids,
          const unordered_map<SlaveID, Resources>& _resources,
          const Resources& _reservation_price, const double _min_price_increase,
          const double _price_multiplier, bool _log_events = true);

  void run();

  const unordered_map<SlaveID, vector<Bid*>>& results() const {
    return winning_bids_per_slave;
  }

 private:
  bool displace(const Bid& new_bid, vector<Bid*>& displaced_bids,
                double& total_cost);

  unordered_map<FrameworkID, vector<vector<Bid>>> all_bids;
  unordered_map<SlaveID, Resources> resources;
  Resources reservation_price;

  unordered_map<SlaveID, vector<Bid*>> winning_bids_per_slave;
  unordered_map<SlaveID, Resources> free_resources_per_slave;

  const double min_price_increase;
  const double price_multiplier;

  bool log_events;
  vector<string> log;
};

#endif  // __AUCTION_HPP__
/* vim: set ts=2 sts=2 sw=2 tw=80 expandtab */
