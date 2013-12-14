import matplotlib.pyplot as plt
import numpy as np
import sys

print(sys.argv)
drf = np.loadtxt(sys.argv[1])
auction = np.loadtxt(sys.argv[2])
roundrobin = np.loadtxt(sys.argv[3])

fig,ax = plt.subplots()
ax.yaxis.grid(b=True, which='both')
ax.set_xticks(range(50))
ax.xaxis.grid(b=True, which='both')

plt.xlabel("time (in hours)")
plt.ylabel("CPU utilization")
plt.title("CPU utilization")
plt.axis([0,90000/3600,0,1])
plt.plot(drf[:,0]/3600.0, drf[:,1]/drf[:,2], label="DRF")
plt.plot(auction[:,0]/3600.0, auction[:,1]/auction[:,2], label="Auction")
plt.plot(roundrobin[:,0]/3600.0, roundrobin[:,1]/roundrobin[:,2], label="Round Robin")

plt.legend(loc="center right") 
plt.show()

fig,ax = plt.subplots()
ax.yaxis.grid(b=True, which='both')
ax.set_xticks(range(50))
ax.xaxis.grid(b=True, which='both')

plt.xlabel("time (in hours)")
plt.ylabel("Memory utilization")
plt.title("Memory utilization")
plt.axis([0,90000/3600,0,1])
plt.plot(drf[:,0]/3600.0, drf[:,3]/drf[:,4], label="DRF")
plt.plot(auction[:,0]/3600.0, auction[:,3]/auction[:,4], label="Auction")
plt.plot(roundrobin[:,0]/3600.0, roundrobin[:,3]/roundrobin[:,4], label="Round Robin")
plt.legend(loc="center right") 
plt.show()
