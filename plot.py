import matplotlib.pyplot as plt
import numpy as np
import sys

print(sys.argv)
drf = np.loadtxt(sys.argv[1])
auction = np.loadtxt(sys.argv[2])
plt.xlabel("time (in seconds)")
plt.ylabel("CPU utilization")
plt.title("CPU utilization")
plt.step(drf[:,0], drf[:,1]/drf[:,2], label="DRF", where='post')
plt.step(auction[:,0], auction[:,1]/auction[:,2], label="Auction", where='post')
plt.legend(loc="center right") 
plt.show()
