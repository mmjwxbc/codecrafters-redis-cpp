#include "Protocol.hpp"
#include "Redis.hpp"
#include "RedisReply.hpp"
#include "TimerEvent.hpp"
#include <arpa/inet.h>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <netdb.h>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

using namespace std;

int main(int argc, char **argv) {
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  string dir, dbfiliname, replicaof;
  int port = -1;
  for (int i = 1; i < argc - 1; ++i) {
    string key = argv[i];
    if (key == "--dir") {
      dir = argv[++i];
    } else if (key == "--dbfilename") {
      dbfiliname = argv[++i];
    } else if (key == "--port") {
      port = stoi(argv[++i]);
    } else if (key == "--replicaof") {
      replicaof = argv[++i];
    }
  }

  port = (port == -1) ? Protocol::DEFAULT_PORT : port;
  bool is_master = replicaof.empty();
  Redis redis(dir, dbfiliname, 0, port, is_master, replicaof);

  const int server_fd = redis.server_fd();

  int epoll_fd = epoll_create1(0);
  if (epoll_fd == -1) {
    perror("epoll_create1");
    exit(EXIT_FAILURE);
  }

  epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = server_fd;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);
  redis.set_epoll_fd(epoll_fd);

  int master_fd = redis.master_fd();
  if (master_fd != -1) {
    vector<RedisReply> master_reply;
    while (true) {
      optional<RedisReply> reply = redis.readOneReply(master_fd);
      if (reply.has_value()) {
        master_reply.emplace_back(reply.value());
      } else {
        break;
      }
    }
    redis.process_command(master_reply, master_fd);
    int flags = fcntl(master_fd, F_GETFL, 0);
    fcntl(master_fd, F_SETFL, flags | O_NONBLOCK);
    epoll_event master_ev;
    master_ev.events = EPOLLIN;
    master_ev.data.fd = master_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, master_fd, &master_ev);
  }

  const int MAX_EVENTS = 64;
  epoll_event events[MAX_EVENTS];

  while (true) {
    int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
    if (nfds == -1) {
      perror("epoll_wait");
      break;
    }

    for (int i = 0; i < nfds; ++i) {
      int fd = events[i].data.fd;
      int wait_timerfd = redis.wait_event_timer_fd();
      int xread_block_timerfd = redis.xread_block_event_timer_fd();
      if (fd == server_fd) {
        int cfd = accept(server_fd, NULL, NULL);
        // set_non_blocking(cfd);
        if (cfd == -1) {
          perror("accept");
          continue;
        }

        epoll_event client_ev;
        client_ev.events = EPOLLIN;
        client_ev.data.fd = cfd;
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, cfd, &client_ev);
      } else if (fd == wait_timerfd) {

        RedisWaitEvent *timer_event = redis.get_wait_timer_event();
        if (timer_event != nullptr) {
          timer_event->on_finish(timer_event->client_fd);
          redis.clear_wait_timer_event();
        }
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
      } else if (fd == xread_block_timerfd) {
        RedisXreadBlockEvent *timer_event = redis.get_xread_block_timer_event();
        if (timer_event != nullptr) {
          timer_event->on_finish(timer_event->client_fd);
          redis.clear_xread_block_timer_event();
        }
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
      } else {
        bool is_close = false;
        vector<RedisReply> replies;

        redis.recv_data(fd, is_close);
        while (true) {
          optional<RedisReply> reply = redis.readOneReply(fd);
          if (reply.has_value()) {
            replies.emplace_back(reply.value());
          } else {
            break;
          }
        }
        redis.process_command(replies, fd);
        if (is_close) {
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
          close(fd);
        }
      }
    }
  }

  close(epoll_fd);
  return 0;
}
