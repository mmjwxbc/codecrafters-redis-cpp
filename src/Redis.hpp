#ifndef REDIS_HH
#define REDIS_HH

#include <netinet/in.h>
#include <ostream>
#include <string>
#include <stdexcept>
#include <netdb.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sstream>
#include <optional>
#include <cctype>
#include <algorithm>
#include <vector>
#include <unordered_map>
#include <chrono>
#include "Protocol.hpp"
#include "RDBParser.hpp"
#include "util.hpp"
#include <filesystem>
#include <cstring>
namespace fs = std::filesystem;

class Redis {
private:
    int sockfd;
    std::string host;
    int port;
    std::stringstream buffer;
    int connection_backlog = 5;
    int cur_db;
    std::vector<std::unordered_map<std::string, std::string>> kvs;
    std::vector<std::unordered_map<std::string, int64_t>> key_elapsed_time_dbs;
    std::unordered_map<std::string, std::string> metadata;
    bool is_master{true};
    std::string master_host;
    int master_port;
    std::vector<int> slave_fds;
    int _master_fd{-1};

public:
    Redis(std::string dir, std::string dbfilename, int cur_db = 0, int port = Protocol::DEFAULT_PORT, bool is_master = true, std::string replicaof = "", const std::string& host = Protocol::DEFAULT_HOST, int connection_backlog = 5)
        : sockfd(-1), host(host), port(port), is_master(is_master), connection_backlog(connection_backlog), cur_db(0), kvs(16), key_elapsed_time_dbs(16){
        if(!dir.empty() && !dbfilename.empty()) {
            fs::path filepath = fs::path(dir) / dbfilename;
            if(fs::exists(filepath)) {
                RDBParser rdb_parser(dir, dbfilename);
                rdb_parser.parseMetadata(metadata);
                rdb_parser.parseDatabase(kvs, key_elapsed_time_dbs);
            }
        }
        if (!replicaof.empty()) {
            int space_pos = replicaof.find(' ');
            master_host = replicaof.substr(0, space_pos);
            master_port = std::stoi(replicaof.substr(space_pos + 1));

            int master_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (master_fd < 0) {
                throw std::runtime_error("socket creation failed");
            }
            struct addrinfo hints{}, *res;
            hints.ai_family = AF_INET;        // IPv4
            hints.ai_socktype = SOCK_STREAM;  // TCP stream socket

            int err = getaddrinfo(master_host.c_str(), std::to_string(master_port).c_str(), &hints, &res);
            if (err != 0) {
                close(master_fd);
                throw std::runtime_error("getaddrinfo failed: " + std::string(gai_strerror(err)));
            }

            if (connect(master_fd, res->ai_addr, res->ai_addrlen) < 0) {
                freeaddrinfo(res);
                close(master_fd);
                throw std::runtime_error("connect failed to master");
            }

            freeaddrinfo(res); // Clean up addrinfo

            // Send PING to master after connection
            sendCommand({makeArray({makeBulk("PING")})}, master_fd);
            char buf[65536];
            std::optional<RedisReply> reply = readReply(master_fd);
            if(reply.has_value()) {
                std::vector<RedisReply> &items = reply.value().elements;
                for(auto &item : items) {
                    if(item.strVal == "PONG") {
                        break;
                    }
                }
            }

            // REPLCONF listening-port <PORT>
            sendCommand({makeArray({makeBulk("REPLCONF"), makeBulk("listening-port"), makeBulk(std::to_string(port))})}, master_fd);
            // REPLCONF capa psync2
            sendCommand({makeArray({makeBulk("REPLCONF"), makeBulk("capa"), makeBulk("psync2")})}, master_fd);
            reply = readReply(master_fd);
            sendCommand({makeArray({makeBulk("PSYNC"), makeBulk("?"), makeBulk("-1")})}, master_fd);
            reply = readReply(master_fd);
            reply = readReply(master_fd);
            recv(master_fd, buf, sizeof(buf), 0);            
            _master_fd = master_fd;
        }
        metadata.insert_or_assign("dir", dir);
        metadata.insert_or_assign("dbfilename", dbfilename);
        metadata.insert_or_assign("master_replid", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        metadata.insert_or_assign("master_repl_offset", "0");
        bind_listen();
    }

    ~Redis() {
        if (sockfd != -1) close(sockfd);
        if(_master_fd != -1) close(_master_fd);
    }
    int server_fd() const {
        return sockfd;
    }

    int master_fd() {
        return _master_fd;
    }
    void bind_listen() {
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        int reuse = 1;
        if (sockfd < 0) throw std::runtime_error("Socket creation failed");
        if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
            throw std::runtime_error("Socket set failed");
        }
        sockaddr_in server_addr {};
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);
        inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr);

        if (bind(sockfd, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            throw std::runtime_error("bind failed");
        }
        if (listen(sockfd, connection_backlog) != 0) {
            throw std::runtime_error("listen failed");
        }
    }

    void sendCommand(const std::vector<RedisReply> &items, const int client_fd) {
        std::cout << "send reply :" << items[0].strVal << std::endl;
        std::string formatted = formatCommand(items);
        // std::cout << formatted << std::endl;
        ::send(client_fd, formatted.c_str(), formatted.size(), 0);
    }


    std::optional<RedisReply> readReply(const int client_fd) {
        char buf[65536];
        ssize_t n = ::recv(client_fd, buf, sizeof(buf), 0);
        if (n < 0) throw std::runtime_error("Read error or connection closed");
        else if (n == 0) {
            return std::nullopt;
        }
        buffer.write(buf, n);
        RedisInputStream ris(buffer);
        return Protocol::read(ris);
    }

    void process_command(RedisReply reply, const int client_fd) {
        std::vector<RedisReply> &items = reply.elements;

        std::string command = items.front().strVal;
        std::transform(command.begin(), command.end(), command.begin(), ::tolower);
        // for(auto item : items) {
        //     std::cout << item.strVal << " " << std::endl;
        // }
        // std::cout << std::endl;
        if (command == "echo") {
            items.erase(items.begin());
            sendCommand(items, client_fd);

        } else if (command == "ping") {
            RedisReply pong;
            pong.type = REPLY_STRING;
            pong.strVal = "PONG";
            sendCommand({pong}, client_fd);

        } else if (command == "set") {
            if (items.size() < 3) return;
            std::string key = items[1].strVal;
            std::string value = items[2].strVal;
            if(items.size() == 5) {
                std::transform(items[3].strVal.begin(), items[3].strVal.end(), command.begin(), ::tolower);
                if(items[3].strVal == "px") {
                    key_elapsed_time_dbs[cur_db].insert_or_assign(key, get_millis() + std::stoi(items[4].strVal));
                }
            }
            // store[key] = value;
            kvs[cur_db].insert_or_assign(key, value);
            // std::cout << "is_master = " << is_master << " SET " << key << " " << value << std::endl;
            if(is_master) {
                RedisReply ok;
                ok.type = REPLY_STRING;
                ok.strVal = "OK";
                sendCommand({ok}, client_fd);
                for(int fd: slave_fds) {
                    RedisReply slave_sync_content;
                    slave_sync_content.elements = reply.elements;
                    slave_sync_content.type = REPLY_ARRAY;
                    sendCommand({reply}, fd);
                    // std::cout << "fd " << fd << " Send Slave:" << " KEY " << key << " VALUE " << value << std::endl;
                }
            }
        } else if (command == "get") {
            if (items.size() < 2) return;
            std::string key = items[1].strVal;
            // std::cout << "is_master = " << is_master << " GET " << key << std::endl; 
            if(key_elapsed_time_dbs[cur_db].count(key)) {
                if(key_elapsed_time_dbs[cur_db][key] <= get_millis()) {
                    key_elapsed_time_dbs[cur_db].erase(key);
                    kvs[cur_db].erase(key);
                }
            }
            RedisReply result;
            if (kvs[cur_db].count(key)) {
                result.type = REPLY_BULK;
                result.strVal = kvs[cur_db][key];
            } else {
                result.type = REPLY_NIL;
            }
            sendCommand({result}, client_fd);
        } else if(command == "config") {
            command = items[1].strVal;
            std::transform(command.begin(), command.end(), command.begin(), ::tolower);
            if(command == "get") {
                auto response = makeArray({
                    makeBulk("dir"),
                    makeBulk(metadata["dir"])
                });
                sendCommand({response}, client_fd);
            }
        } else if(command == "keys") {
            std::string pattern = items[1].strVal;
            RedisReply reply;
            reply.type = RedisReplyType::REPLY_ARRAY;
            for (const auto& [key, _] : kvs[cur_db]) {
                if(matchPattern(pattern, key)) {
                    reply.elements.emplace_back(std::move(makeBulk(key)));
                }
            }
            sendCommand({reply}, client_fd);
        } else if(command == "info") {
            std::string &arg = items[1].strVal;
            std::transform(arg.begin(), arg.end(), arg.begin(), ::tolower);
            if (arg == "replication") {
                std::ostringstream oss;
                if (is_master) {
                    oss << "role:master\n";
                    oss << "master_replid:" << metadata["master_replid"] << "\n";
                    oss << "master_repl_offset:" << metadata["master_repl_offset"] << "\n";
                } else {
                    oss << "role:slave\n";
                }
                sendCommand({makeBulk(oss.str())}, client_fd);
            }
        } else if(command == "replconf") {
            sendCommand({makeString("OK")}, client_fd);
        } else if(command == "psync") {
            sendCommand({makeString("FULLRESYNC " + metadata["master_replid"] + " " + metadata["master_repl_offset"])}, client_fd);
            const std::string empty_rdb = "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76\x65"
            "\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69\x74\x73"
            "\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08\x75\x73\x65"
            "\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73"
            "\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2";
            std::string content = "$" + std::to_string(empty_rdb.size()) + "\r\n" + empty_rdb;
            if(send(client_fd, content.c_str(), content.size(), 0) != content.size()) {
                throw std::runtime_error("send RDB failed");
            }
            slave_fds.emplace_back(client_fd);
            // std::cout << "slave client fd = " << client_fd << std::endl;
        }
    }

private:
    std::string formatCommand(const std::vector<RedisReply>& items) {
        std::ostringstream oss;
        for (const auto& r : items) {
            switch (r.type) {
                case REPLY_STRING:
                    oss << "+" << r.strVal << "\r\n";
                    break;
                case REPLY_ERROR:
                    oss << "-" << r.strVal << "\r\n";
                    break;
                case REPLY_INTEGER:
                    oss << ":" << r.intVal << "\r\n";
                    break;
                case REPLY_BULK:
                    oss << "$" << r.strVal.size() << "\r\n" << r.strVal << "\r\n";
                    break;
                case REPLY_NIL:
                    oss << "$-1\r\n";
                    break;
                case REPLY_ARRAY:
                    oss << "*" << r.elements.size() << "\r\n";
                    for (const auto& sub : r.elements) {
                        oss << formatCommand({sub});
                    }
                    break;
            }
        }
        return oss.str();
    }

    int64_t get_millis() {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    }

    RedisReply makeBulk(const std::string& s) {
        RedisReply r;
        r.type = REPLY_BULK;
        r.strVal = s;
        return r;
    }

    RedisReply makeArray(const std::vector<RedisReply>& elements) {
        RedisReply r;
        r.type = REPLY_ARRAY;
        r.elements = elements;
        return r;
    }

    RedisReply makeString(const std::string &s) {
        RedisReply r;
        r.type = REPLY_STRING;
        r.strVal = s;
        return r;
    }
};

#endif // REDIS_HH
