#include "Protocol.hpp"
#include "Redis.hpp"
#include <arpa/inet.h>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
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
    if (key == "--dir") { // key starts with --
      dir = argv[i + 1];
      ++i; // Skip value
    } else if (key == "--dbfilename") {
      dbfiliname = argv[i + 1];
      ++i;
    } else if (key == "--port") {
      port = stoi(argv[i + 1]);
      ++i;
    } else if (key == "--replicaof") {
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
  if (master_fd != -1) {
    if (master_fd > max_fd) {
      max_fd = master_fd;
    }
    FD_SET(master_fd, &fdset);
  }
  while (true) {
    fd_set tmp = fdset;
    int ret = select(max_fd + 1, &tmp, NULL, NULL, NULL);
    if (FD_ISSET(server_fd, &tmp)) {
      int cfd = accept(server_fd, NULL, NULL);
      FD_SET(cfd, &fdset);
      max_fd = cfd > max_fd ? cfd : max_fd;
    }
    for (int i = 0; i < max_fd + 1; i++) {
      if (i != server_fd && FD_ISSET(i, &tmp)) {
        bool is_close = false;
        while (true) {
          optional<RedisReply> reply = redis.readOneReply(i, is_close);

          if (reply.has_value()) {
            redis.process_command({reply.value()}, i);
          }
          if (is_close) {
            FD_CLR(i, &fdset);
            close(i);
            break;
          }
          if (reply.has_value() && !redis.buffer_has_more_data(i)) {
            break;
          }
        }
      }
    }

    return 0;
  }
}