import matplotlib.pyplot as plt
import numpy as np

drf = np.loadtxt("drf_time")
auction = np.loadtxt("auction_time")
plt.xlabel("time (in seconds)")
plt.ylabel("CPU utilization")
plt.title("CPU utilization")
plt.plot(drf[:,0], drf[:,1]/drf[:,2], label="DRF")
plt.plot(auction[:,0], auction[:,1]/auction[:,2], label="Auction")
plt.legend(loc="center right") 
plt.show()
