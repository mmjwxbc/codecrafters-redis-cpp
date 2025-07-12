#include <optional>
#include <cstdint>
#include <iostream>
#include <fstream>
#include <filesystem> 
#include <string>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
class RDBParser {
    private:
        std::string dir;
        std::string dbfilename;
        std::ifstream file;
    public:
        RDBParser(std::string dir, std::string dbfilename) : dir(dir), dbfilename(dbfilename) {
            fs::path filepath = fs::path(dir) / dbfilename;
            file = std::ifstream(filepath, std::ios::binary);
        }
        uint8_t readByte() {
            char byte;
            file.read(&byte, 1);
            return static_cast<uint8_t>(byte);
        }

        void read_type_key_value(std::vector<std::unordered_map<std::string, std::string>> &kv, std::vector<std::unordered_map<std::string, int64_t>> &elapsed_time_kv, std::optional<uint64_t> mills, int cur_db) {
            uint8_t type = readByte();
            if(type == 0x00) {
                std::string key = readString();
                std::string value = readString();
                kv[cur_db].insert_or_assign(key, value);
                elapsed_time_kv[cur_db].insert_or_assign(key, mills);
            }
        }

        uint64_t readMills() {
            uint64_t mills = 0;
            for(int i = 0; i < 8; i++) {
                uint64_t byte = static_cast<uint64_t>(readByte());
                mills |= byte << (i * 8);
            }
            return mills;
        }

        uint64_t readSeconds() {
            uint64_t secs = 0;
            for(int i = 0; i < 8; i++) {
                uint64_t byte = static_cast<uint64_t>(readByte());
                secs |= byte << (i * 8);
            }
            secs *= 1000u;
            return secs;
        }

        // 读取 string-encoded 字符串（开头是 size encoded）
        std::string readString() {
            uint8_t first = readByte();

            // Case 1: 普通 6-bit 长度字符串（0b00 开头）
            if ((first & 0xC0) == 0x00) {
                int len = first & 0x3F;
                std::string result(len, '\0');
                file.read(&result[0], len);
                return result;
            }
            // Case 2: 14-bit 长度字符串（0b01）
            else if ((first & 0xC0) == 0x40) {
                uint8_t second = readByte();
                int len = ((first & 0x3F) << 8) | second;
                std::string result(len, '\0');
                file.read(&result[0], len);
                return result;
            }
            // Case 3: 特殊整数编码
            else if (first == 0xC0) {
                uint8_t val = readByte();
                return std::to_string(val);
            } else if (first == 0xC1) {
                uint8_t b1 = readByte();
                uint8_t b2 = readByte();
                int val = b1 | (b2 << 8);
                return std::to_string(val);
            } else if (first == 0xC2) {
                uint8_t b1 = readByte();
                uint8_t b2 = readByte();
                uint8_t b3 = readByte();
                uint8_t b4 = readByte();
                int val = b1 | (b2 << 8) | (b3 << 16) | (b4 << 24);
                return std::to_string(val);
            }

            throw std::runtime_error("Unsupported string encoding");
        }
    
        uint64_t readLength(std::ifstream &file) {
            uint8_t first = readByte();

            // Case 1: 00xxxxxx
            if ((first & 0xC0) == 0x00) {
                return first & 0x3F;
            }

            // Case 2: 01xxxxxx
            else if ((first & 0xC0) == 0x40) {
                uint8_t second = readByte();
                return ((first & 0x3F) << 8) | second;
            }

            // Case 3: 10xxxxxx
            else if ((first & 0xC0) == 0x80) {
                uint8_t b[4];
                file.read(reinterpret_cast<char*>(b), 4);
                return (static_cast<uint64_t>(b[0]) << 24) |
                    (static_cast<uint64_t>(b[1]) << 16) |
                    (static_cast<uint64_t>(b[2]) << 8) |
                    (static_cast<uint64_t>(b[3]));
            }

            // Case 4: 11xxxxxx — special encoding
            else {
                throw std::runtime_error("Special encoding not allowed for length");
            }
        }

        void parseMetadata(std::unordered_map<std::string, std::string> &metadata) {
            while (true) {
                uint8_t type = readByte();
                if (type != 0xFA) {
                    file.unget();
                    break;
                }

                std::string key = readString();
                std::string value = readString();

                metadata.insert_or_assign(key, value);
            }
        }

        void parseDatabase(std::vector<std::unordered_map<std::string, std::string>> &kv, std::vector<std::unordered_map<std::string, int64_t>> &elapsed_time_kv) {
            while(true) {
                uint8_t type = readByte();
                if(type == 0xFE) {
                    uint8_t cur_db = readByte();
                    if(readByte() != 0xFB) {
                        throw std::runtime_error("parseDatabse hash table size failed");
                        uint64_t kv_size = readLength(file);
                        uint64_t elapsed_time_kv_size = readLength(file);
                        while(true) {
                            uint8_t type = readByte();
                            if(type == 0xFE || type == 0xFF) {
                                file.unget();
                                break;
                            }
                            if(type == 0x00) {
                                std::string key = readString();
                                std::string value = readString();
                                kv[cur_db].insert_or_assign(key, value);
                            } else if(type == 0xFC) {
                                uint64_t mills = readMills();
                                type = readByte();
                                read_type_key_value(kv, elapsed_time_kv, mills, cur_db);
                            } else if(type == 0xFD) {
                                uint64_t mills = readSeconds();
                                read_type_key_value(kv, elapsed_time_kv, mills, cur_db);
                            }
                        }
                    }
                } else {
                    file.unget();
                    break;
                }
            }
        }
};
