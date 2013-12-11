#ifndef __SHARED_HPP__
#define __SHARED_HPP__

#include <algorithm>
#include <string>
#include <vector>
#include <sstream>

#define cpuperram 0.5

#define DEBUG 0

//#define RR
#define DRF
//#define AUCTION

typedef size_t SlaveID;
typedef size_t TaskID;
typedef size_t FrameworkID;

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

  double weight() const { return cpus + mem * cpuperram; }

  bool zero() const { return cpus == 0 && mem == 0 && disk == 0; }

  double sum() const { return cpus + mem + disk; }

  std::string tostring() const {
    std::stringstream ss;
    ss << "{cpus=" << cpus;
    ss << ", mem=" << mem;
    ss << ", disk=" << disk << "}";
    return ss.str();
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
  return left.cpus > right.cpus && left.mem > right.mem &&
         left.disk > right.disk;
}

inline bool operator<(const Resources& left, const Resources& right) {
  return left.cpus < right.cpus && left.mem < right.mem &&
         left.disk < right.disk;
}

inline bool operator>=(const Resources& left, const Resources& right) {
  return left.cpus >= right.cpus && left.mem >= right.mem &&
         left.disk >= right.disk;
}

inline bool operator<=(const Resources& left, const Resources& right) {
  return left.cpus <= right.cpus && left.mem <= right.mem &&
         left.disk <= right.disk;
}

template <typename T> class Indexer {
 public:
  Indexer() {}

  template <class... Args> T& add(Args&& ... args) {
    objects.emplace_back(args...);
    T& added = objects.back();
    added.set_id(objects.size() - 1);
    return added;
  }

  void reserve(size_t n) { objects.reserve(n); }

  T& get(size_t id) { return objects[id]; }
  const T& get(size_t id) const { return objects[id]; }

  T& operator[](size_t id) { return objects[id]; }
  const T& operator[](size_t id) const { return objects[id]; }

  size_t size() const { return objects.size(); }
  typename std::vector<T>::iterator begin() { return objects.begin(); }
  typename std::vector<T>::const_iterator begin() const {
    return objects.begin();
  }
  typename std::vector<T>::iterator end() { return objects.end(); }
  typename std::vector<T>::const_iterator end() const { return objects.end(); }

 private:
  std::vector<T> objects;
};

class Indexable {
 public:
  size_t id() const { return _id; }
  void set_id(size_t id) { _id = id; }

 private:
  size_t _id;
};

template <typename T>
std::string join(const std::vector<T>& elems, const char* delim) {
  if (elems.empty()) 
    return "";

  std::stringstream ss;
  ss << elems[0];
  for (int i = 1; i < elems.size(); ++i) {
    ss << delim << elems[i];
  }
  return ss.str();
}

inline std::ostream& operator<<(std::ostream& o, const Resources& resources) {
  o << resources.tostring();
  return o;
}

// Split a string of doubles separated by delim into a vector<double>.
std::vector<double> split(const std::string& str, char delim);

// Check whether [s1, e1] intersects [s2, e2].
bool intersect(double s1, double e1, double s2, double e2);

// Comparator for pointer types.
// Taken from
// <http://stackoverflow.com/questions/1517854/priority-queue-comparison-for-pointers>.
template <typename Type, typename Compare = std::less<Type> >
struct pless : public std::binary_function<Type*, Type*, bool> {
  bool operator()(const Type* x, const Type* y) const {
    return Compare()(*x, *y);
  }
};

#endif  // __SHARED_HPP__
/* vim: set ts=2 sts=2 sw=2 tw=80 expandtab */
