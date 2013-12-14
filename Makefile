CXX = g++
CXXFLAGS = -g -O4 -march=native -std=c++0x
LIBS = -lglog -lgflags

mesosSim : mesosSim.o auction.o shared.o mesos.o
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LIBS)

auction.o : auction.cpp auction.hpp shared.hpp

mesosSim.o : mesosSim.cpp auction.hpp shared.hpp

shared.o : shared.cpp shared.hpp

mesos.o : mesos.cpp mesos.hpp shared.hpp

clean::
	$(RM) *.o
