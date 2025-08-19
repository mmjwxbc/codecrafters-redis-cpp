#ifndef REDIS_SETT_HPP
#define REDIS_SETT_HPP
#include <string>
#include <unordered_map>
#include <set>

typedef struct SkipNode {
    std::string member;
    double score;
} SkipNode;

struct SortedSet {
    std::unordered_map<std::string, double> member_score; // 快速查分数
    std::set<std::pair<double, std::string>> score_member;     // 排序
};


#endif