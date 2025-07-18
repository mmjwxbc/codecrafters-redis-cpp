#pragma once
#include <string>
#include <optional>
#include "RedisInputStream.hpp"
#include "RedisReply.hpp"

class Protocol {
public:
    inline static const std::string DEFAULT_HOST = "127.0.0.1";
    inline static const int DEFAULT_PORT = 6379;

    static std::optional<RedisReply> read(RedisInputStream& is);

    static std::optional<int64_t> readBulkStringlen(RedisInputStream& is);

private:
    static std::optional<RedisReply> readRESP(RedisInputStream& is);
    static std::optional<RedisReply> processBulkString(RedisInputStream& is);
    static std::optional<RedisReply> processArray(RedisInputStream& is);
    static std::optional<RedisReply> processInteger(RedisInputStream& is);
    static std::optional<RedisReply> processSimpleString(RedisInputStream& is);
    static std::optional<RedisReply> processError(RedisInputStream& is);
};