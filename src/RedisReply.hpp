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


struct RedisServerReply {
    RedisReply reply;
    int client_fd;

    RedisServerReply(RedisReply reply, int client_fd) : reply(std::move(reply)), client_fd(client_fd) {} 
};