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
#include "Protocol.hpp"

class Redis {
private:
    int sockfd;
    std::string host;
    int port;
    std::stringstream buffer;  // 用于包装为 RedisInputStream
    int connection_backlog = 5;
    

public:
    Redis(const std::string& host = Protocol::DEFAULT_HOST, int port = Protocol::DEFAULT_PORT, int connection_backlog = 5)
        : sockfd(-1), host(host), port(port), connection_backlog(connection_backlog) {
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
        std::cout << "formatted : " << formatted << std::endl;
        ::send(client_fd, formatted.c_str(), formatted.size(), 0);
    }


    std::optional<RedisReply> readReply(const int client_fd) {
        char buf[65536];
        ssize_t n = ::recv(client_fd, buf, sizeof(buf), 0);
        if (n < 0) throw std::runtime_error("Read error or connection closed");
        else if (n == 0) {
            // close(client_fd);
            return std::nullopt; // 显式返回空值
        }
        buffer.write(buf, n);
        RedisInputStream ris(buffer);
        return Protocol::read(ris);
    }

    void process_command(RedisReply reply, const int client_fd) {
        std::vector<RedisReply> &items = reply.elements;
        
        transform(items.front().strVal.begin(),items.front().strVal.end(),items.front().strVal.begin(),::tolower);
        if(items.front().strVal == "ECHO") {
            items.erase(items.begin());
            sendCommand(items, client_fd);
        } else if(items.front().strVal == "PING") {
            RedisReply redisreply{};
            redisreply.strVal = "PONG";
            sendCommand({redisreply}, client_fd);
        }
    }

    // // Example command wrappers
    // void set(const std::string& key, const std::string& value) {
    //     sendCommand("SET " + key + " " + value);
    //     RedisReply reply = readReply();
    //     // 可加断言 reply.type == REPLY_STRING && reply.strVal == "OK"
    // }

    // std::string get(const std::string& key) {
    //     sendCommand("GET " + key);
    //     RedisReply reply = readReply();
    //     if (reply.type == REPLY_BULK) return reply.strVal;
    //     throw std::runtime_error("GET failed or key not found");
    // }

private:
    std::string formatCommand(const std::vector<RedisReply>& items) {
        std::ostringstream oss;
        // oss << "*" << parts.size() << "\r\n";
        for (const auto& p : items) {
            oss << "$" << p.strVal.size() << "\r\n" << p.strVal << "\r\n";
        }
        return oss.str();
    }
};

#endif // REDIS_HH
