#ifndef __SHARED_HPP__
#define __SHARED_HPP__

#include <algorithm>
#include <string>
#include <vector>

typedef unsigned int SlaveID;
typedef unsigned int TaskID;
typedef unsigned int FrameworkID;

struct Resources {
	double cpus;
	double mem;
	double disk;

  Resources() : cpus(0), mem(0), disk(0) {}
  Resources(double cpus_, double mem_, double disk_)
    : cpus(cpus_), mem(mem_), disk(disk_) {}

  Resources& operator+=(const Resources& other) {
    cpus += other.cpus;
    mem += other.mem;
    disk += other.disk;
    return *this;
  }

  Resources& operator-=(const Resources& other) {
    cpus -= other.cpus;
    mem -= other.mem;
    disk -= other.disk;
    return *this;
  }

  // Compute non-negative X such that this + X >= other.
  Resources missing(const Resources& other) const {
    Resources result;
    result.cpus = std::max(other.cpus - cpus, 0.);
    result.mem = std::max(other.mem - mem, 0.);
    result.disk = std::max(other.disk - disk, 0.);
    return result;
  }

  bool zero() const {
    return cpus == 0 && mem == 0 && disk == 0;
  }

  double sum() const {
    return cpus + mem + disk;
  }
};

inline Resources operator+(Resources left, const Resources& right) {
  left.cpus += right.cpus;
  left.mem += right.mem;
  left.disk += right.disk;
  return left;
}

inline Resources operator-(Resources left, const Resources& right) {
  left.cpus -= right.cpus;
  left.mem -= right.mem;
  left.disk -= right.disk;
  return left;
}

inline Resources operator*(Resources left, const Resources& right) {
  left.cpus *= right.cpus;
  left.mem *= right.mem;
  left.disk *= right.disk;
  return left;
}

inline bool operator>(const Resources& left, const Resources& right) {
  return left.cpus > right.cpus && left.mem > right.mem && left.disk > right.disk;
}

inline bool operator<(const Resources& left, const Resources& right) {
  return left.cpus < right.cpus && left.mem < right.mem && left.disk < right.disk;
}

inline bool operator>=(const Resources& left, const Resources& right) {
  return left.cpus >= right.cpus && left.mem >= right.mem && left.disk >= right.disk;
}

inline bool operator<=(const Resources& left, const Resources& right) {
  return left.cpus <= right.cpus && left.mem <= right.mem && left.disk <=
    right.disk;
}

// Split a string of doubles separated by delim into a vector<double>.
std::vector<double> split(const std::string& str, char delim);

// Check whether [s1, e1] intersects [s2, e2].
bool intersect(double s1, double e1, double s2, double e2);

// Comparator for pointer types.
// Taken from <http://stackoverflow.com/questions/1517854/priority-queue-comparison-for-pointers>.
template<typename Type, typename Compare = std::less<Type> >
struct pless : public std::binary_function<Type *, Type *, bool> {
    bool operator()(const Type *x, const Type *y) const
        { return Compare()(*x, *y); }
};

#endif  // __SHARED_HPP__
