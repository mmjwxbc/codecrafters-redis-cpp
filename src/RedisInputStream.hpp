#pragma once
#include <string>
#include <optional>

class RedisInputStream {
private:
    std::string& input;
    size_t cursor;

public:
    RedisInputStream(std::string& in) : input(in), cursor(0) {}

    std::string& buffer() {
        return input;
    }

    std::optional<char> readByte() {
        if (cursor >= input.size()) return std::nullopt;
        return input[cursor++];
    }

    std::optional<std::string> readLine() {
        size_t end = input.find("\r\n", cursor);
        if (end == std::string::npos) return std::nullopt;
        std::string line = input.substr(cursor, end - cursor);
        cursor = end + 2;
        line.pop_back();
        line.pop_back();
        return line;
    }

    std::optional<int64_t> readLongCrLf() {
        auto line = readLine();
        if (!line) return std::nullopt;
        try {
            return std::stoll(*line);
        } catch (...) {
            return std::nullopt;
        }
    }

    std::optional<std::string> readBulkBody(size_t len) {
        if (cursor + len + 2 > input.size()) return std::nullopt;
        std::string body = input.substr(cursor, len);
        cursor += len + 2;  // skip \r\n
        return body;
    }

    size_t getCursor() const { return cursor; }
    void resetCursor(size_t pos) { cursor = pos; }
};
