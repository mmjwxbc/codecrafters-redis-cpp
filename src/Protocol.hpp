#ifndef PROTOCOL_HH
#define PROTOCOL_HH

#include <cassert>
#include <istream>
#include <string>
#include <vector>
#include <stdint.h>
#include <stdexcept>
#include <iostream>

// Redis response types
enum RedisReplyType {
    REPLY_STRING,
    REPLY_BULK,
    REPLY_INTEGER,
    REPLY_ARRAY,
    REPLY_NIL,
    REPLY_ERROR
};

// RedisReply 表示 Redis 返回的多种类型数据
struct RedisReply {
    RedisReplyType type;
    std::string strVal;
    int64_t intVal;
    std::vector<RedisReply> elements;
    size_t len;

    RedisReply() : type(REPLY_NIL), intVal(0) {}
    RedisReply(std::string strVal) : type(REPLY_STRING), strVal(strVal) {}
};

// RedisInputStream 简化版，使用 std::istream 包装
class RedisInputStream {
private:
    std::istream& input;
public:
    RedisInputStream(std::istream& in) : input(in) {}

    std::istream& input_stream() const{
        return input;
    }

    char readByte() {
        char c;
        input.get(c);
        if (input.eof()) throw std::runtime_error("Unexpected EOF");
        return c;
    }

    std::string readLine() {
        std::string line;
        std::getline(input, line);
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();  // remove \r
        }
        return line;
    }

    int readIntCrLf() {
        return std::stoi(readLine());
    }

    int64_t readLongCrLf() {
        return std::stoll(readLine());
    }

    std::string readBulkString(int len) {
        std::string data(len, '\0');
        input.read(&data[0], len);
        char cr = readByte();
        char lf = readByte();
        if (cr != '\r' || lf != '\n') throw std::runtime_error("Bad bulk string ending");
        return data;
    }
};

// Protocol
class Protocol {
private:
    Protocol();

    static std::vector<unsigned char> toByteArray(int value) {
        std::string val = std::to_string(value);
        return std::vector<unsigned char>(val.begin(), val.end());
    }

    static std::vector<unsigned char> encode(const std::string& str) {
        return std::vector<unsigned char>(str.begin(), str.end());
    }

public:
    inline static const std::string DEFAULT_HOST = "127.0.0.1";
    inline static const int DEFAULT_PORT = 6379;

    inline static const unsigned char DOLLAR_BYTE   = '$';
    inline static const unsigned char ASTERISK_BYTE = '*';
    inline static const unsigned char PLUS_BYTE     = '+';
    inline static const unsigned char MINUS_BYTE    = '-';
    inline static const unsigned char COLON_BYTE    = ':';

    inline static const std::vector<unsigned char> BYTES_TRUE     = toByteArray(1);       // "1"
    inline static const std::vector<unsigned char> BYTES_FALSE    = toByteArray(0);       // "0"
    inline static const std::vector<unsigned char> BYTES_TILDE    = encode("~");          // "~"
    inline static const std::vector<unsigned char> BYTES_EQUAL    = encode("=");          // "="
    inline static const std::vector<unsigned char> BYTES_ASTERISK = encode("*");          // "*"


    static RedisReply read(RedisInputStream& is) {
        return process(is);
    }

    static int processBulkStringlen(RedisInputStream& is) {
        char type = is.readByte();
        assert(type == '$');
        return is.readIntCrLf();
    }

private:
    static RedisReply process(RedisInputStream& is) {
        RedisReply reply;
        auto start = is.input_stream().tellg();
        
        char b = is.readByte();
        switch (b) {
            case '+': reply = processSimpleString(is); break;
            case '-': reply = processError(is); break;
            case ':': reply = processInteger(is); break;
            case '$': reply = processBulkString(is); break;
            case '*': reply = processArray(is); break;
            default:
                throw std::runtime_error(std::string("Unknown reply type: ") + b);
        }
        auto end = is.input_stream().tellg();
        reply.len = static_cast<size_t>(end - start) + 1;
        return reply;
    }

    static RedisReply processSimpleString(RedisInputStream& is) {
        RedisReply reply;
        reply.type = REPLY_STRING;
        reply.strVal = is.readLine();
        return reply;
    }

    static RedisReply processError(RedisInputStream& is) {
        RedisReply reply;
        reply.type = REPLY_ERROR;
        reply.strVal = is.readLine();
        return reply;
    }

    static RedisReply processInteger(RedisInputStream& is) {
        RedisReply reply;
        reply.type = REPLY_INTEGER;
        reply.intVal = is.readLongCrLf();
        return reply;
    }

    static RedisReply processBulkString(RedisInputStream& is) {
        int len = is.readIntCrLf();
        RedisReply reply;
        if (len == -1) {
            reply.type = REPLY_NIL;
            return reply;
        }
        reply.type = REPLY_BULK;
        reply.strVal = is.readBulkString(len);
        return reply;
    }

    static RedisReply processArray(RedisInputStream& is) {
        RedisReply reply;
        int count = is.readIntCrLf();
        if (count == -1) {
            reply.type = REPLY_NIL;
            return reply;
        }
        reply.type = REPLY_ARRAY;
        for (int i = 0; i < count; ++i) {
            reply.elements.push_back(process(is));
        }
        return reply;
    }
};
#endif // PROTOCOL_HH
