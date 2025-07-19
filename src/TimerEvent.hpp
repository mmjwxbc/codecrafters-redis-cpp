// RedisWaitEvent.hpp
#ifndef REDIS_WAIT_EVENT_HPP
#define REDIS_WAIT_EVENT_HPP

#include <chrono>
#include <functional>
#include <unordered_set>
struct RedisWaitEvent {
    int timerfd;
    // 客户端的 fd，执行 WAIT 命令的客户端
    int client_fd;

    // 要等待的副本数量
    int required_acks;

    int inacks;
    std::unordered_set<int> ack_fds; 
    // 超时时间点（绝对时间，用于超时检查）
    std::chrono::steady_clock::time_point expire_time;

    // 完成时调用（如向客户端返回结果）
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

#endif // REDIS_WAIT_EVENT_HPP
