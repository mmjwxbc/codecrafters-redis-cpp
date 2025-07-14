#include <cstddef>
#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/select.h>
#include "Protocol.hpp"
#include "Redis.hpp"
#include <vector>

using namespace std;

int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  string dir, dbfiliname, replicaof;
  int port = -1;
  for (int i = 1; i < argc - 1; ++i) {
    string key = argv[i];
    if (key == "--dir") {  // key starts with --
        dir = argv[i + 1];
        ++i; // Skip value
    } else if(key == "--dbfilename") {
      dbfiliname = argv[i + 1];
      ++i;
    } else if(key == "--port") {
      port = stoi(argv[i + 1]);
      ++i;
    } else if(key == "--replicaof") {
      replicaof = argv[i + 1];
      ++i;
    }
  }
  port = (port == -1) ? Protocol::DEFAULT_PORT : port;
  bool is_master = replicaof.empty();
  Redis redis(dir, dbfiliname, 0, port, is_master, replicaof);
  const int server_fd = redis.server_fd();
  fd_set fdset;
  FD_ZERO(&fdset);
  FD_SET(server_fd, &fdset);
  int max_fd = server_fd;
  int master_fd = redis.master_fd();
  if(master_fd != -1) {
    if(master_fd > max_fd) {
      max_fd = master_fd;
    }
    FD_SET(master_fd, &fdset);
  }
  while(true) {
    fd_set tmp = fdset;
    int ret = select(max_fd + 1, &tmp, NULL, NULL, NULL);
    if(FD_ISSET(server_fd, &tmp)) {
      int cfd = accept(server_fd, NULL, NULL);
      FD_SET(cfd, &fdset);
      max_fd = cfd > max_fd ? cfd : max_fd;
    }
    for(int i = 0; i < max_fd + 1; i++) {
      if(i != server_fd && FD_ISSET(i, &tmp)) {
        while (true) {
          vector<RedisReply> reply = redis.readAllAvailableReplies(i);
          if(reply.empty()) {
            FD_CLR(i, &fdset);
            close(i);
            break;
          } else {
            redis.process_command(reply, i);
          }
          // 只要buffer里还有未处理数据就继续处理
          if (!redis.buffer_has_more_data(i)) break;
        }
      }
    }
  }

  return 0;
}
