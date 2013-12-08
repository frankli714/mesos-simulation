#include <cassert>
#include <string>
#include <vector>

#include "shared.hpp"

using namespace std;

vector<double> split(const string& str, char delim) {
  vector<double> split_v;
  int i = 0;
  string buf = "";
  while (i < str.length()) {
    if (str[i] != delim)
      buf += str[i];
    else {
      split_v.push_back(atof(buf.c_str()));
      buf = "";
    }
    i++;
  }
  if (!buf.empty()) split_v.push_back(atof(buf.c_str()));
  return split_v;
}

bool intersect(double s1, double e1, double s2, double e2) {
  assert(s1 <= e1);
  assert(s2 <= e2);

  if (s1 > e2 || s2 > e1) {
    return false;
  }
  return true;
}
/* vim: set ts=2 sts=2 sw=2 tw=80 expandtab */
