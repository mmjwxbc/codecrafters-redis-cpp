#ifndef REDIS_HH
#define REDIS_HH

#include <cstddef>
#include <cstdint>
#include <netinet/in.h>
#include <optional>
#include <ostream>
#include <string>
#include <stdexcept>
#include <netdb.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sstream>
#include <cctype>
#include <algorithm>
#include <utility>
#include <vector>
#include <unordered_map>
#include <chrono>
#include "Protocol.hpp"
#include "RedisInputStream.hpp"
#include "RedisReply.hpp"
#include "RDBParser.hpp"
#include "util.hpp"
#include <filesystem>
#include <cstring>
#include "TimerEvent.hpp"
#include <sys/epoll.h>
#include <sys/timerfd.h>
namespace fs = std::filesystem;

class Redis {
private:
    int sockfd;
    std::string host;
    int port;
    std::unordered_map<int, std::string> buffers;
    int connection_backlog = 5;
    int cur_db{0};
    std::vector<std::unordered_map<std::string, std::string>> kvs;
    std::vector<std::unordered_map<std::string, int64_t>> key_elapsed_time_dbs;
    std::unordered_map<std::string, std::string> metadata;
    std::string master_host;
    int master_port;
    std::vector<int> slave_fds;
    std::unordered_map<int, size_t> slave_offsets;
    bool is_master{true};
    int _master_fd{-1};
    int epoll_fd{-1};
    size_t processed_bytes{0};
    RedisWaitEvent *timer_event{nullptr};

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
            bool is_close = false;
            int space_pos = replicaof.find(' ');
            master_host = replicaof.substr(0, space_pos);
            master_port = std::stoi(replicaof.substr(space_pos + 1));

            int master_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (master_fd < 0) {
                throw std::runtime_error("socket creation failed");
            }

            int reuse = 1;
            if (setsockopt(master_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
                throw std::runtime_error("set sockopt failed\n");
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
            set_non_blocking(master_fd);

            freeaddrinfo(res); // Clean up addrinfo

            // Send PING to master after connection
            sendReply({makeArray({makeBulk("PING")})}, master_fd);
            std::optional<RedisReply> reply = readOneReply(master_fd);
            // std::cout << reply.strVal << std::endl
            while(not reply.has_value()) {
                recv_data(master_fd, is_close);
                reply = readOneReply(master_fd);
            }
            if(reply.value().strVal != "PONG") {
                std::cout << reply.value().strVal << std::endl;
                throw std::runtime_error("SLAVE PING FAILED");
            }
            // REPLCONF listening-port <PORT>
            sendReply({makeArray({makeBulk("REPLCONF"), makeBulk("listening-port"), makeBulk(std::to_string(port))})}, master_fd);
            // REPLCONF capa psync2
            reply = readOneReply(master_fd);
            while(not reply.has_value()) {
                recv_data(master_fd, is_close);
                reply = readOneReply(master_fd);
            }
            if(reply.value().strVal != "OK") {
                std::cout << reply.value().strVal << std::endl;
                throw std::runtime_error("LISTENING-PORT FAILED");
            }
            sendReply({makeArray({makeBulk("REPLCONF"), makeBulk("capa"), makeBulk("psync2")})}, master_fd);
            // std::cout << reply.strVal << std::endl;
            reply = readOneReply(master_fd);
            while(not reply.has_value()) {
                recv_data(master_fd, is_close);
                reply = readOneReply(master_fd);
            }
            // std::cout << reply.strVal << std::endl;
            if(reply.value().strVal != "OK") {
                std::cout << reply.value().strVal << std::endl;
                throw std::runtime_error("LISTENING-PORT FAILED");
            }
            sendReply({makeArray({makeBulk("PSYNC"), makeBulk("?"), makeBulk("-1")})}, master_fd);
            reply = readOneReply(master_fd);
            while(not reply.has_value()) {
                recv_data(master_fd, is_close);
                reply = readOneReply(master_fd);
            }
            std::cout << reply.value().strVal << std::endl;

            auto rdb_len = readBulkStringLen(master_fd);
            while(not rdb_len.has_value()) {
                recv_data(master_fd, is_close);
                rdb_len = readBulkStringLen(master_fd);
            }
            std::string& master_buffer = buffers[master_fd];

            auto readable_bytes_in_buffer = master_buffer.size();


            if (readable_bytes_in_buffer < rdb_len) {
                size_t bytes_to_read_from_kernel = static_cast<size_t>(rdb_len.value()) - readable_bytes_in_buffer;
                
                std::vector<char> temp_buf(65536);
                size_t total_received_from_kernel = 0;

                while (total_received_from_kernel < bytes_to_read_from_kernel) {
                    ssize_t n = ::recv(master_fd, temp_buf.data(), temp_buf.size(), 0);

                    if (n < 0) {
                        throw std::runtime_error("recv error while filling buffer for RDB");
                    }
                    if (n == 0) {
                        throw std::runtime_error("connection closed before RDB fully received");
                    }

                    master_buffer.append(temp_buf.data(), n);
                    
                    total_received_from_kernel += n;
                }
            }
            std::string rdb_data;
            rdb_data = master_buffer.substr(0, rdb_len.value());
            master_buffer.erase(0, rdb_len.value());
            _master_fd = master_fd;
            std::cout << "Receive RDB FILE" << std::endl;
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

    void set_epoll_fd(int epoll_fd) {
        this->epoll_fd = epoll_fd;
    }

    int timer_fd() const {
        if(timer_event != nullptr) {
            return timer_event->timerfd;
        }
        return -1;
    }

    RedisWaitEvent *get_timer_event() const {
        return timer_event;
    }

    void clear_timer_event() {
        if(timer_event != nullptr) {
            delete timer_event;
            timer_event = nullptr;
        }
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

    void sendReply(const std::vector<RedisReply> &items, const int client_fd) {
        std::string formatted = formatReply(items);
        slave_offsets[client_fd] += formatted.size();
        // if (is_master && slave_offsets.find(client_fd) != slave_offsets.end()) {
        //     processed_bytes += formatted.size();
        //     metadata["master_repl_offset"] = std::to_string(processed_bytes);
        // }
        ::send(client_fd, formatted.c_str(), formatted.size(), 0);
    }

    std::optional<int64_t> readBulkStringLen(const int client_fd) {
        RedisInputStream is(buffers[client_fd]);
        std::optional<int64_t> len = Protocol::readBulkStringlen(is);
        return len;
    }

    bool buffer_has_more_data(int client_fd) {
        return !buffers[client_fd].empty();
    }

    void recv_data(const int client_fd, bool &is_close) {
        char buf[65536];
        ssize_t n = ::recv(client_fd, buf, sizeof(buf), 0);
        if (n > 0) {
            buffers[client_fd].append(buf, n);
        } else if (n <= 0) {
            // EOF
            is_close = true;
        }
    }

    std::optional<RedisReply> readOneReply(const int client_fd) {
        RedisInputStream is(buffers[client_fd]);
        return Protocol::read(is);
    }


    void process_command(std::vector<RedisReply> replys, const int client_fd) {
        for(auto &reply : replys) {

            std::vector<RedisReply> &items = reply.elements;
            std::string command = items.front().strVal;
            std::transform(command.begin(), command.end(), command.begin(), ::tolower);
            std::cout << "*****" << std::endl;
            for(auto item : items) {
                std::cout << item.strVal << " " << std::endl;
            }
            std::cout << "*****" << std::endl;
            if (command == "echo") {
                items.erase(items.begin());
                sendReply(items, client_fd);

            } else if (command == "ping" && client_fd != _master_fd) {
                RedisReply pong;
                pong.type = REPLY_STRING;
                pong.strVal = "PONG";
                sendReply({pong}, client_fd);
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
                    sendReply({ok}, client_fd);
                    for(int fd: slave_fds) {
                        sendReply({reply}, fd);
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
                sendReply({result}, client_fd);
            } else if(command == "config") {
                command = items[1].strVal;
                std::transform(command.begin(), command.end(), command.begin(), ::tolower);
                if(command == "get") {
                    auto response = makeArray({
                        makeBulk("dir"),
                        makeBulk(metadata["dir"])
                    });
                    sendReply({response}, client_fd);
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
                sendReply({reply}, client_fd);
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
                    sendReply({makeBulk(oss.str())}, client_fd);
                }
            } else if(command == "replconf") {
                if (items.size() > 1) {
                    std::string &arg = items[1].strVal;
                    std::transform(arg.begin(), arg.end(), arg.begin(), ::tolower);
                    if(arg == "getack") {
                        sendReply({makeArray({makeBulk("REPLCONF"), makeBulk("ACK"), makeBulk(std::to_string(processed_bytes))})}, client_fd);
                    } else if(arg == "ack") {
                        if(timer_event != nullptr) {
                            if(timer_event->ack_fds.count(client_fd) == 0)  {
                                int64_t offset = std::stoll(items[2].strVal);
                                slave_offsets[client_fd] = offset;
                                if(offset >= processed_bytes) {
                                    timer_event->ack_fds.insert(client_fd);
                                    timer_event->inacks++;
                                }
                            }
                        }
                        if(timer_event != nullptr && timer_event->ack_fds.size() == timer_event->required_acks) {
                            timer_event->on_finish(timer_event->client_fd);
                            clear_timer_event();
                        }
                    } else {
                        sendReply({makeString("OK")}, client_fd);
                    }
                } else {
                    sendReply({makeString("OK")}, client_fd);
                }
            } else if(command == "psync") {
                sendReply({makeString("FULLRESYNC " + metadata["master_replid"] + " " + metadata["master_repl_offset"])}, client_fd);
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
                slave_offsets.insert(std::make_pair(client_fd, 0));
                // std::cout << "slave client fd = " << client_fd << std::endl;
            } else if(command == "wait") {
                if(items.size() < 3) return;
                int numreplicas = std::stoi(items[1].strVal);
                int timeout = std::stoi(items[2].strVal);
                
                // Check if there are any pending write operations
                bool has_pending_operations = false;
                for(int fd : slave_fds) {
                    if(slave_offsets[fd] < processed_bytes) {
                        has_pending_operations = true;
                        break;
                    }
                }
                
                // If no pending operations, return immediately with 0
                if(!has_pending_operations) {
                    sendReply({makeInterger(0)}, client_fd);
                    return;
                }
                
                // Send GETACK to all replicas to check their current offset
                for(int fd : slave_fds) {
                    std::cout << "SEND GETACK TO " << fd << std::endl;
                    sendReply({makeArray({makeBulk("REPLCONF"), makeBulk("GETACK"), makeBulk("*")})}, fd);
                }
                
                int timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
                itimerspec it = {
                    .it_interval = {0, 0},
                    .it_value = {0, timeout}
                };
                timerfd_settime(timerfd, 0, &it, nullptr);
                epoll_event ev;
                ev.events = EPOLLIN;
                ev.data.fd = timerfd;
                timer_event = new RedisWaitEvent(timerfd, client_fd, numreplicas, std::chrono::milliseconds(timeout));
                timer_event->on_finish = [this](int client_fd) {
                    sendReply({makeInterger(timer_event->inacks)}, client_fd);
                };
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, timerfd, &ev);
            }
            // std::cout << "processed_bytes = " << processed_bytes << std::endl;
            if(client_fd == _master_fd) {
                processed_bytes += reply.len;
            }
        }
        
    }

private:
    std::string formatReply(const std::vector<RedisReply>& items) {
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
                        oss << formatReply({sub});
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

    RedisReply makeInterger(const int val) {
        RedisReply r;
        r.type = REPLY_INTEGER;
        // r.strVal = s;
        r.intVal = val;
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
