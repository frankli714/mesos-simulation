#ifndef __SHARED_HPP__
#define __SHARED_HPP__

#include <algorithm>

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
    cpus += other.cpus;
    mem += other.mem;
    disk += other.disk;
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
#endif  // __SHARED_HPP__
