#ifndef UTIL_HH
#define UTIL_HH
#include <string>
bool matchPattern(const std::string &, const std::string &);
void escapeCRLF(const std::string&);
void set_non_blocking(const int fd);
uint64_t currentTimeMillis();
bool isNumber(const std::string&);
bool unsupport_command(const std::string&);
int check_longitude_latitude(double, double);
std::string formatErrorLonLat(double, double);
#endif