#pragma once
#include <string>
#include <vector>
#include <cstdint>

enum RedisReplyType {
    REPLY_STRING,
    REPLY_BULK,
    REPLY_INTEGER,
    REPLY_ARRAY,
    REPLY_NIL,
    REPLY_ERROR
};

struct RedisReply {
    RedisReplyType type;
    std::string strVal;
    int64_t intVal;
    std::vector<RedisReply> elements;
    size_t len;
};

struct RedisStreamEntry {
    std::string id;
    std::string key;
    std::string value;
    RedisStreamEntry(const std::string &id_, const std::string &key_, const std::string &value_)
        : id(id_), key(key_), value(value_) {}
};