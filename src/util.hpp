#ifndef UTIL_HH
#define UTIL_HH
#include <string>

struct Coordinates {
    double latitude;
    double longitude;
};

bool matchPattern(const std::string &, const std::string &);
void escapeCRLF(const std::string&);
void set_non_blocking(const int fd);
uint64_t currentTimeMillis();
bool isNumber(const std::string&);
bool unsupport_command(const std::string&);
int check_longitude_latitude(double, double);
std::string formatErrorLonLat(double, double);
uint64_t encode(double latitude, double longitude);
uint64_t encode(double latitude, double longitude);
Coordinates decode(uint64_t geo_code);
double geohashGetDistance(double lon1d, double lat1d, double lon2d, double lat2d);
#endif