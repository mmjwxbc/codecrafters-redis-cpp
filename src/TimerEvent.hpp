// RedisWaitEvent.hpp
#ifndef REDIS_WAIT_EVENT_HPP
#define REDIS_WAIT_EVENT_HPP

#include <chrono>
#include <functional>
#include <unordered_set>
#include <string>
#include <queue>


struct RedisWaitEvent {
    int timerfd;
    int client_fd;

    int required_acks;

    int inacks;
    std::unordered_set<int> ack_fds; 
    std::chrono::steady_clock::time_point expire_time;

    std::function<void(int)> on_finish;

    RedisWaitEvent(int timerfd_,
                   int client_fd_,
                   int required_acks_,
                   std::chrono::milliseconds timeout
                  )
        : timerfd(timerfd_),
          client_fd(client_fd_),
          required_acks(required_acks_),
          expire_time(std::chrono::steady_clock::now() + timeout),
          inacks(0) {}
};

struct RedisXreadBlockEvent {
    int timerfd;
    int client_fd;
    std::string stream_key;
    std::string id;


    std::chrono::steady_clock::time_point expire_time;

    std::function<void(int)> on_finish;

    RedisXreadBlockEvent(int timerfd_,
                   int client_fd_,
                   std::string stream_key,
                   std::string id,
                   std::chrono::milliseconds timeout
                  )
        : timerfd(timerfd_),
          client_fd(client_fd_),
          stream_key(stream_key),
          id(id),
          expire_time(std::chrono::steady_clock::now() + timeout)
          {}
};

struct RedisBlpopEvent {
    std::string list_key;
    int client_fd;
    std::chrono::steady_clock::time_point expire_time;


    // std::function<void(int)> on_finish;

    RedisBlpopEvent(int client_fd,
                   std::string list_key,
                   std::chrono::steady_clock::time_point expire_time)
        : client_fd(client_fd),
          list_key(list_key),
          expire_time(expire_time) {}
};

#endif // REDIS_WAIT_EVENT_HPP
