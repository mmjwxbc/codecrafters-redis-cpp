#ifndef REDIS_HH
#define REDIS_HH

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

public:
    Redis(std::string dir, std::string dbfilename, int cur_db = 0, const std::string& host = Protocol::DEFAULT_HOST, int port = Protocol::DEFAULT_PORT, int connection_backlog = 5)
        : sockfd(-1), host(host), port(port), connection_backlog(connection_backlog), cur_db(0), kvs(16), key_elapsed_time_dbs(16){
        if(!dir.empty() && !dbfilename.empty()) {
            fs::path filepath = fs::path(dir) / dbfilename;
            if(fs::exists(filepath)) {
                RDBParser rdb_parser(dir, dbfilename);
                rdb_parser.parseMetadata(metadata);
                rdb_parser.parseDatabase(kvs, key_elapsed_time_dbs);
            }
        }
        metadata.insert_or_assign("dir", dir);
        metadata.insert_or_assign("dbfilename", dbfilename);
        bind_listen();
    }

    ~Redis() {
        if (sockfd != -1) close(sockfd);
    }
    int server_fd() const {
        return sockfd;
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
            RedisReply ok;
            ok.type = REPLY_STRING;
            ok.strVal = "OK";
            sendCommand({ok}, client_fd);
        } else if (command == "get") {
            if (items.size() < 2) return;
            std::string key = items[1].strVal;
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

};

#endif // REDIS_HH
