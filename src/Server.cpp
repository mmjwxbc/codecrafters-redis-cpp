// #include "Protocol.hpp"
// #include "Redis.hpp"
// #include "RedisReply.hpp"
// #include <arpa/inet.h>
// #include <cstddef>
// #include <cstdio>
// #include <cstdlib>
// #include <cstring>
// #include <iostream>
// #include <netdb.h>
// #include <string>
// #include <sys/select.h>
// #include <sys/socket.h>
// #include <sys/types.h>
// #include <unistd.h>
// #include <vector>

// using namespace std;

// int main(int argc, char **argv) {
//   // Flush after every std::cout / std::cerr
//   std::cout << std::unitbuf;
//   std::cerr << std::unitbuf;
//   string dir, dbfiliname, replicaof;
//   int port = -1;
//   for (int i = 1; i < argc - 1; ++i) {
//     string key = argv[i];
//     if (key == "--dir") { // key starts with --
//       dir = argv[i + 1];
//       ++i; // Skip value
//     } else if (key == "--dbfilename") {
//       dbfiliname = argv[i + 1];
//       ++i;
//     } else if (key == "--port") {
//       port = stoi(argv[i + 1]);
//       ++i;
//     } else if (key == "--replicaof") {
//       replicaof = argv[i + 1];
//       ++i;
//     }
//   }
//   port = (port == -1) ? Protocol::DEFAULT_PORT : port;
//   bool is_master = replicaof.empty();
//   Redis redis(dir, dbfiliname, 0, port, is_master, replicaof);
//   const int server_fd = redis.server_fd();
//   fd_set fdset;
//   FD_ZERO(&fdset);
//   FD_SET(server_fd, &fdset);
//   int max_fd = server_fd;
//   int master_fd = redis.master_fd();
//   if (master_fd != -1) {
//     cout << "master fd = " << master_fd << endl;
//     vector<RedisReply> master_reply;
//     while (true) {
//       optional<RedisReply> reply = redis.readOneReply(master_fd);
//       if (reply.has_value()) {
//         master_reply.emplace_back(reply.value());
//       } else {
//         break;
//       }
//     }
//     redis.process_command(master_reply, master_fd);
//     if (master_fd > max_fd) {
//       max_fd = master_fd;
//     }
//     FD_SET(master_fd, &fdset);
//   }
//   while (true) {
//     fd_set tmp = fdset;
//     int ret = select(max_fd + 1, &tmp, NULL, NULL, NULL);
//     if (FD_ISSET(server_fd, &tmp)) {
//       int cfd = accept(server_fd, NULL, NULL);
//       set_non_blocking(cfd);
//       cout << "client fd = " << cfd << endl;
//       FD_SET(cfd, &fdset);
//       max_fd = cfd > max_fd ? cfd : max_fd;
//     }
//     for (int i = 0; i < max_fd + 1; i++) {
//       if (i != server_fd && FD_ISSET(i, &tmp)) {
//         bool is_close = false;
//         vector<RedisReply> replies;
//         redis.recv_data(i, is_close);
//         while(true) {
//           optional<RedisReply> reply = redis.readOneReply(i);
//           if(reply.has_value()) {
//             replies.emplace_back(reply.value());
//           } else {
//             break;
//           } 
//           if(is_close) {
//             break;
//           }
//         }
//         redis.process_command(replies, i);
//         if (is_close) {
//           FD_CLR(i, &fdset);
//           close(i);
//           break;
//         }
//       }
//     }
//   }
//   return 0;
// }

#include "Protocol.hpp"
#include "Redis.hpp"
#include "RedisReply.hpp"
#include <arpa/inet.h>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <fcntl.h>


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

  // 1. 创建 epoll 实例
  int epoll_fd = epoll_create1(0);
  if (epoll_fd == -1) {
    perror("epoll_create1");
    exit(EXIT_FAILURE);
  }

  // 2. 注册 server_fd 到 epoll 中
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
    cout << "master fd = " << master_fd << endl;
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
      int timerfd = redis.timer_fd();
      if (fd == server_fd) {
        int cfd = accept(server_fd, NULL, NULL);
        // set_non_blocking(cfd);
        if (cfd == -1) {
          perror("accept");
          continue;
        }
        cout << "client fd = " << cfd << endl;

        epoll_event client_ev;
        client_ev.events = EPOLLIN;
        client_ev.data.fd = cfd;
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, cfd, &client_ev);
      } else if(fd == timerfd) {
         RedisWaitEvent *timer_event = redis.get_timer_event();
         if(timer_event != nullptr) {
            timer_event->on_finish(timer_event->client_fd);
            delete timer_event;
            timer_event = nullptr;
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
