#include "Protocol.hpp"
#include "RedisInputStream.hpp"
#include <cstdint>
#include <optional>

std::optional<RedisReply> Protocol::read(RedisInputStream& is) {
    size_t mark = is.getCursor();
    auto reply = readRESP(is);
    if (!reply.has_value()) {
        is.resetCursor(mark);  // rollback on failure
    } else {
        is.buffer().erase(0, is.getCursor());
    }
    return reply;
}

std::optional<int64_t> Protocol::readBulkStringlen(RedisInputStream& is) {
    size_t mark = is.getCursor();
    auto len = is.readLongCrLf();
    if(!len.has_value()) {
        is.resetCursor(mark);
    } else {
        is.buffer().erase(0, is.getCursor());
    }
    return len;
}

std::optional<RedisReply> Protocol::readRESP(RedisInputStream& is) {
    auto opt_b = is.readByte();
    if (!opt_b.has_value()) return std::nullopt;
    char b = *opt_b;

    switch (b) {
        case '+': return processSimpleString(is);
        case '-': return processError(is);
        case ':': return processInteger(is);
        case '$': return processBulkString(is);
        case '*': return processArray(is);
        default: return std::nullopt;
    }
}

std::optional<RedisReply> Protocol::processSimpleString(RedisInputStream& is) {
    auto line = is.readLine();
    if (!line) return std::nullopt;
    RedisReply r;
    r.type = REPLY_STRING;
    r.strVal = *line;
    return r;
}

std::optional<RedisReply> Protocol::processError(RedisInputStream& is) {
    auto line = is.readLine();
    if (!line) return std::nullopt;
    RedisReply r;
    r.type = REPLY_ERROR;
    r.strVal = *line;
    return r;
}

std::optional<RedisReply> Protocol::processInteger(RedisInputStream& is) {
    auto val = is.readLongCrLf();
    if (!val.has_value()) return std::nullopt;
    RedisReply r;
    r.type = REPLY_INTEGER;
    r.intVal = *val;
    return r;
}

std::optional<RedisReply> Protocol::processBulkString(RedisInputStream& is) {
    auto len = is.readLongCrLf();
    if (!len.has_value()) return std::nullopt;

    if (*len == -1) {
        RedisReply r;
        r.type = REPLY_NIL;
        return r;
    }

    auto body = is.readBulkBody(*len);
    if (!body) return std::nullopt;

    RedisReply r;
    r.type = REPLY_BULK;
    r.strVal = *body;
    return r;
}

std::optional<RedisReply> Protocol::processArray(RedisInputStream& is) {
    auto count = is.readLongCrLf();
    if (!count.has_value()) return std::nullopt;

    RedisReply r;
    r.type = REPLY_ARRAY;

    for (int i = 0; i < *count; ++i) {
        auto element = readRESP(is);
        if (!element.has_value()) return std::nullopt;
        r.elements.push_back(*element);
    }

    return r;
}
