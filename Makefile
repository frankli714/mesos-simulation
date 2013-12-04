CXX = g++
CXXFLAGS = -g -std=c++0x

mesosSim : mesosSim.o auction.o
	$(CXX) $(CXXFLAGS) $^ -o $@

auction.o : auction.cpp auction.hpp shared.hpp

mesosSim.o : mesosSim.cpp auction.hpp shared.hpp

clean::
	$(RM) *.o
