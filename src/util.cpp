#include <cctype>
#include <string>
#include <iostream>
#include <fcntl.h>
#include <chrono>
#include <cstdint>
#include <unordered_set>
#include <algorithm>
#include <sstream>
#include <iomanip>

bool matchPattern(const std::string &pattern, const std::string &text) {
    size_t p = 0, t = 0, star = std::string::npos, match = 0;

    while (t < text.size()) {
        if (p < pattern.size() && (pattern[p] == '?' || pattern[p] == text[t])) {
            ++p; ++t;
        } else if (p < pattern.size() && pattern[p] == '*') {
            star = p++;
            match = t;
        } else if (star != std::string::npos) {
            p = star + 1;
            t = ++match;
        } else {
            return false;
        }
    }

    while (p < pattern.size() && pattern[p] == '*') ++p;
    return p == pattern.size();
}

void escapeCRLF(const std::string& input) {
    std::string out;
    for (char c : input) {
        if (c == '\r') out += "\\r";
        else if (c == '\n') out += "\\n";
        else out += c;
    }
    std::cout << out << std::endl;
}


void set_non_blocking(const int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl get");
        exit(1);
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl set");
        exit(1);
    }
}


uint64_t currentTimeMillis() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

bool isNumber(const std::string& str) {
    return !str.empty() && std::all_of(str.begin(), str.end(), ::isdigit);
}


bool unsupport_command(const std::string& cmd) {
    static const std::unordered_set<std::string> commands = {
        "psubscribe", "subscribe", "punsubscribe", "unsubscribe", "ping", "quit", "reset"
    };
    return commands.count(cmd) > 0;
}

int check_longitude_latitude(double longitude, double latitude) {
    int error_ret = 0;
    if(longitude < -180 || longitude > 180) {
        error_ret += 1;
    }
    if(latitude < -85.05112878 || latitude > 85.05112878) {
        error_ret += 2;
    }
    return error_ret;
}

std::string formatErrorLonLat(double lon, double lat) {
    // (error) ERR invalid longitude,latitude pair 180.000000,90.000000
    std::ostringstream oss;
    oss <<  "ERR invalid longitude,latitude pair "<<std::fixed << std::setprecision(6) << lon << "," << lat;
    return oss.str();
}