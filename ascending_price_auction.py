from itertools import chain

def ascending_bid_auction(resources, bidders, minimum_ratio):
    bids = set()
    while True:
        new_bids = set.union(*(bidder(bids) for bidder in bidders))
        winning_bids = allocation(resources, bids | new_bids)
        bids |= {bid.scale(minimum_ratio) for bid in winning_bids & new_bids}
        if not winning_bids & new_bids:
            return { bid.scale(1.0/minimum_ratio) for bid in winning_bids }
            
def ascending_proxy_auction(resources, bids):
   return ascending_bid_auction(resources, {proxy_bidder(bid) for bid in bids})
   
def allocation(resources, bids):
# approximate solution to knapsack problem; pick a feasible set of jobs that maximizes revenue
    pass

def cost(resources, bids, package):
    return efficiency(resources, bids) - efficiency(resources - package, bids)

def efficiency(resources, bids):
    return sum(map(lambda bid : bid.value, allocation(resources, bids)))

def proxy_bidder(resources, bid, minimum_increment):
    return lambda bids : proxy_bid(resources, bids, bid, minimum_increment)

def proxy_bid(resources, bids, bid, minimum_increment):
    price = cost(resources, bids, bid.package) + minimum_increment
    return set([Bid(package=bids.package, value=price)]) if price <= bid.value else set()

class Bid:
    def __init__(self, package, value):
        self.pakage = package
        self.value = value
    
    def scale(self, ratio):
        return Bid(package=self.package, value=self.value*ratio)
    
    def __eq__(self, other): 
        return self.value == other.value and self.package == other.package
    
    def __hash__(self):
        return self.package * 123123 + int(100000 * self.value)

