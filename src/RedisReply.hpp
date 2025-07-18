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