#ifndef REDIS_HH
#define REDIS_HH

#include "Protocol.hpp"
#include "RDBParser.hpp"
#include "RedisInputStream.hpp"
#include "RedisReply.hpp"
#include "Stream.hpp"
#include "TimerEvent.hpp"
#include "util.hpp"
#include "RedisSet.hpp"
#include <algorithm>
#include <arpa/inet.h>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iterator>
#include <netdb.h>
#include <netinet/in.h>
#include <optional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>
#include <algorithm>
#include <set>
using namespace std;

class Redis {
private:
  int sockfd;
  string host;
  int port;
  unordered_map<int, string> buffers;
  int connection_backlog = 5;
  int cur_db{0};
  vector<unordered_map<string, string>> kvs;
  vector<unordered_map<string, int64_t>> key_elapsed_time_dbs;
  unordered_map<string, string> metadata;
  string master_host;
  int master_port;
  vector<int> slave_fds;
  unordered_map<int, size_t> slave_offsets;
  bool is_master{true};
  int _master_fd{-1};
  int epoll_fd{-1};
  size_t processed_bytes{0};
  RedisWaitEvent *wait_timer_event{nullptr};
  RedisXreadBlockEvent *xread_block_timer_event{nullptr};
  unordered_map<string, SimpleStream> streams;
  unordered_map<int, vector<RedisReply>> multi_queue;
  unordered_map<string, deque<string>> key_lists;
  unordered_map<string, vector<RedisBlpopEvent*>> blpop_events_by_key;
  set<pair<chrono::steady_clock::time_point, RedisBlpopEvent*>> blpop_events_by_time;
  unordered_map<string, set<int>> channel_subscribers;
  unordered_map<int, set<string>> client_subscribe_channels;
  unordered_map<string, SortedSet> zsets;


public:
  Redis(string dir, string dbfilename, int cur_db = 0,
        int port = Protocol::DEFAULT_PORT, bool is_master = true,
        string replicaof = "",
        const string &host = Protocol::DEFAULT_HOST,
        int connection_backlog = 5)
      : sockfd(-1), host(host), port(port), is_master(is_master),
        connection_backlog(connection_backlog), cur_db(0), kvs(16),
        key_elapsed_time_dbs(16) {
    if (!dir.empty() && !dbfilename.empty()) {
      fs::path filepath = fs::path(dir) / dbfilename;
      if (fs::exists(filepath)) {
        RDBParser rdb_parser(dir, dbfilename);
        rdb_parser.parseMetadata(metadata);
        rdb_parser.parseDatabase(kvs, key_elapsed_time_dbs);
      }
    }
    if (!replicaof.empty()) {
      bool is_close = false;
      int space_pos = replicaof.find(' ');
      master_host = replicaof.substr(0, space_pos);
      master_port = stoi(replicaof.substr(space_pos + 1));

      int master_fd = socket(AF_INET, SOCK_STREAM, 0);
      if (master_fd < 0) {
        throw runtime_error("socket creation failed");
      }

      int reuse = 1;
      if (setsockopt(master_fd, SOL_SOCKET, SO_REUSEADDR, &reuse,
                     sizeof(reuse)) < 0) {
        throw runtime_error("set sockopt failed\n");
      }

      struct addrinfo hints{}, *res;
      hints.ai_family = AF_INET;       // IPv4
      hints.ai_socktype = SOCK_STREAM; // TCP stream socket

      int err = getaddrinfo(master_host.c_str(),
                            to_string(master_port).c_str(), &hints, &res);
      if (err != 0) {
        close(master_fd);
        throw runtime_error("getaddrinfo failed: " +
                                 string(gai_strerror(err)));
      }

      if (connect(master_fd, res->ai_addr, res->ai_addrlen) < 0) {
        freeaddrinfo(res);
        close(master_fd);
        throw runtime_error("connect failed to master");
      }
      set_non_blocking(master_fd);

      freeaddrinfo(res); // Clean up addrinfo

      // Send PING to master after connection
      sendReply(makeArray({makeBulk("PING")}), master_fd);
      optional<RedisReply> reply = readOneReply(master_fd);
      // cout << reply.strVal << endl
      while (not reply.has_value()) {
        recv_data(master_fd, is_close);
        reply = readOneReply(master_fd);
      }
      if (reply.value().strVal != "PONG") {
        // cout << reply.value().strVal << endl;
        throw runtime_error("SLAVE PING FAILED");
      }
      // REPLCONF listening-port <PORT>
      sendReply(makeArray({makeBulk("REPLCONF"), makeBulk("listening-port"),
                           makeBulk(to_string(port))}),
                master_fd);
      // REPLCONF capa psync2
      reply = readOneReply(master_fd);
      while (not reply.has_value()) {
        recv_data(master_fd, is_close);
        reply = readOneReply(master_fd);
      }
      if (reply.value().strVal != "OK") {
        // cout << reply.value().strVal << endl;
        throw runtime_error("LISTENING-PORT FAILED");
      }
      sendReply(makeArray({makeBulk("REPLCONF"), makeBulk("capa"),
                           makeBulk("psync2")}),
                master_fd);
      // cout << reply.strVal << endl;
      reply = readOneReply(master_fd);
      while (not reply.has_value()) {
        recv_data(master_fd, is_close);
        reply = readOneReply(master_fd);
      }
      // cout << reply.strVal << endl;
      if (reply.value().strVal != "OK") {
        // cout << reply.value().strVal << endl;
        throw runtime_error("LISTENING-PORT FAILED");
      }
      sendReply(makeArray({makeBulk("PSYNC"), makeBulk("?"), makeBulk("-1")}),
                master_fd);
      reply = readOneReply(master_fd);
      while (not reply.has_value()) {
        recv_data(master_fd, is_close);
        reply = readOneReply(master_fd);
      }
      // cout << reply.value().strVal << endl;

      auto rdb_len = readBulkStringLen(master_fd);
      while (not rdb_len.has_value()) {
        recv_data(master_fd, is_close);
        rdb_len = readBulkStringLen(master_fd);
      }
      string &master_buffer = buffers[master_fd];

      auto readable_bytes_in_buffer = master_buffer.size();

      if (readable_bytes_in_buffer < rdb_len) {
        size_t bytes_to_read_from_kernel =
            static_cast<size_t>(rdb_len.value()) - readable_bytes_in_buffer;

        vector<char> temp_buf(65536);
        size_t total_received_from_kernel = 0;

        while (total_received_from_kernel < bytes_to_read_from_kernel) {
          ssize_t n = ::recv(master_fd, temp_buf.data(), temp_buf.size(), 0);

          if (n < 0) {
            throw runtime_error("recv error while filling buffer for RDB");
          }
          if (n == 0) {
            throw runtime_error(
                "connection closed before RDB fully received");
          }

          master_buffer.append(temp_buf.data(), n);

          total_received_from_kernel += n;
        }
      }
      string rdb_data;
      rdb_data = master_buffer.substr(0, rdb_len.value());
      master_buffer.erase(0, rdb_len.value());
      _master_fd = master_fd;
      // cout << "Receive RDB FILE" << endl;
    }
    metadata.insert_or_assign("dir", dir);
    metadata.insert_or_assign("dbfilename", dbfilename);
    metadata.insert_or_assign("master_replid",
                              "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
    metadata.insert_or_assign("master_repl_offset", "0");
    bind_listen();
  }

  ~Redis() {
    if (sockfd != -1)
      close(sockfd);
    if (_master_fd != -1)
      close(_master_fd);
  }
  int server_fd() const { return sockfd; }

  void set_epoll_fd(int epoll_fd) { this->epoll_fd = epoll_fd; }

  int wait_event_timer_fd() const {
    if (wait_timer_event != nullptr) {
      return wait_timer_event->timerfd;
    }
    return -1;
  }

  int xread_block_event_timer_fd() const {
    if (xread_block_timer_event != nullptr) {
      return xread_block_timer_event->timerfd;
    }
    return -1;
  }


  RedisWaitEvent *get_wait_timer_event() const { return wait_timer_event; }
  RedisXreadBlockEvent *get_xread_block_timer_event() const {
    return xread_block_timer_event;
  }


  void clear_wait_timer_event() {
    if (wait_timer_event != nullptr) {
      delete wait_timer_event;
      wait_timer_event = nullptr;
    }
  }

  void clear_xread_block_timer_event() {
    if (xread_block_timer_event != nullptr) {
      delete xread_block_timer_event;
      xread_block_timer_event = nullptr;
    }
  }

  int master_fd() { return _master_fd; }
  void bind_listen() {
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    if (sockfd < 0)
      throw runtime_error("Socket creation failed");
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
        0) {
      throw runtime_error("Socket set failed");
    }
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr);

    if (bind(sockfd, (sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
      throw runtime_error("bind failed");
    }
    if (listen(sockfd, connection_backlog) != 0) {
      throw runtime_error("listen failed");
    }
  }

  void sendReply(const RedisReply &item, const int client_fd) {
    string formatted = formatReply(item);
    ::send(client_fd, formatted.c_str(), formatted.size(), 0);
  }

  optional<int64_t> readBulkStringLen(const int client_fd) {
    RedisInputStream is(buffers[client_fd]);
    optional<int64_t> len = Protocol::readBulkStringlen(is);
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

  optional<RedisReply> readOneReply(const int client_fd) {
    RedisInputStream is(buffers[client_fd]);
    return Protocol::read(is);
  }

  void process_request(vector<RedisReply> replies, const int client_fd) {
    for (auto reply : replies) {
      auto server_replies = process_command(reply, client_fd);
      for (auto server_reply : server_replies) {
        sendReply(server_reply.reply, server_reply.client_fd);
      }
    }
  }

  int process_blpop_timeout_event() {
    if(blpop_events_by_time.empty()) {
      return -1;
    }
    auto now = chrono::steady_clock::now();

    while (!blpop_events_by_time.empty()) {
        auto it = blpop_events_by_time.begin();
        auto expire_time = it->first;
        RedisBlpopEvent* ev = it->second;

        if (expire_time > now) {
            break;
        }

        // 事件过期，处理之
        blpop_events_by_time.erase(it);

        // 移除从 key-index 索引中（例如 unordered_map<string, vector<event>>）
        auto& vec = blpop_events_by_key[ev->list_key];
        vec.erase(remove(vec.begin(), vec.end(), ev), vec.end());

        // 触发超时处理逻辑（返回 nil，关闭阻塞等）
        handle_blpop_timeout(ev->client_fd);

        delete ev;
    }

    if (!blpop_events_by_time.empty()) {
        auto next_expire_time = blpop_events_by_time.begin()->first;
        auto wait_duration = duration_cast<chrono::milliseconds>(next_expire_time - now).count();
        return max<int>(wait_duration, 0); // 防止负数
    }

    return -1; // 没有事件
  }

private:
  void check_xread_block_event(string stream_key, string stream_id) {
    if (xread_block_timer_event == nullptr) {
      return;
    }
    if (stream_key != xread_block_timer_event->stream_key) {
      return;
    }
    if (stream_id > xread_block_timer_event->id) {
      vector<RedisReply> single_stream_reply = AssembleStreamResults(
          xread_block_timer_event->client_fd,
          xread_block_timer_event->stream_key, xread_block_timer_event->id);
      sendReply({makeArray({makeArray(single_stream_reply)})},
                xread_block_timer_event->client_fd);
      clear_xread_block_timer_event();
    }
  }

  vector<RedisReply> AssembleStreamResults(const int client_fd,
                                                string stream_key,
                                                string start_id) {
    auto results = streams[stream_key].xread(start_id);
    vector<RedisReply> single_stream_reply;
    single_stream_reply.emplace_back(makeString(stream_key));

    vector<RedisReply> entries_array;
    for (const auto &result : results) {
      vector<RedisReply> entry_reply;
      entry_reply.emplace_back(makeString(result.entry_id));

      vector<RedisReply> fields_array;
      for (const auto &[field, value] : result.fields) {
        fields_array.emplace_back(makeString(field));
        fields_array.emplace_back(makeString(value));
      }

      entry_reply.emplace_back(makeArray(fields_array));
      entries_array.emplace_back(makeArray(entry_reply));
    }
    single_stream_reply.emplace_back(makeArray(entries_array));
    return single_stream_reply;
  }
  void exec_multi_queue(vector<RedisReply> replies, const int client_fd) {
    vector<RedisReply> totals;
    for (auto reply : replies) {
      auto server_replies = process_command(reply, client_fd);
      for (auto server_reply : server_replies) {
        totals.emplace_back(server_reply.reply);
      }
    }
    sendReply(makeArray(totals), client_fd);
  }
  vector<RedisServerReply> process_command(RedisReply reply,
                                                const int client_fd) {
    vector<RedisReply> &items = reply.elements;
    string command = items.front().strVal;
    transform(command.begin(), command.end(), command.begin(), ::tolower);
    // cout << "*****" << endl;
    // for (auto item : items) {
    //   cout << item.strVal << " " << endl;
    // }
    // cout << "*****" << endl;
    vector<RedisServerReply> server_replies;
    // cout << "client fd = " << client_fd << " " << items[0].strVal << "in sub = " << (client_subscribe_channels.find(client_fd) != client_subscribe_channels.end()) <<  endl;
    if(client_subscribe_channels.find(client_fd) != client_subscribe_channels.end()) {
      if(unsupport_command(command) == false) { 
        server_replies.emplace_back(makeError("ERR Can't execute \'" + command + "\': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"), client_fd);
        goto end;
      } else if(command == "ping") {
        server_replies.emplace_back(makeArray({makeBulk("pong"), makeBulk("")}), client_fd);
        goto end;
      }
    }
    if (multi_queue.find(client_fd) != multi_queue.end() && command != "exec" &&
        command != "discard") {
      // cout << "in multi mode" << endl;
      multi_queue[client_fd].emplace_back(reply);
      //   sendReply({makeString("QUEUED")}, client_fd);
      server_replies.emplace_back(makeString("QUEUED"), client_fd);
    } else if (command == "discard") {
      if (multi_queue.find(client_fd) != multi_queue.end()) {
        multi_queue.erase(client_fd);
        server_replies.emplace_back(makeString("OK"), client_fd);
      } else {
        server_replies.emplace_back(makeError("ERR DISCARD without MULTI"),
                                    client_fd);
      }
    } else if (command == "exec") {
      if (multi_queue.find(client_fd) != multi_queue.end()) {
        if (multi_queue[client_fd].empty()) {
          // cout << "multi queue empty" << endl;
          multi_queue.erase(client_fd);
          //   sendReply({makeArray({})}, client_fd);
          server_replies.emplace_back(makeArray({}), client_fd);
        } else {
          vector<RedisReply> replys = std::move(multi_queue[client_fd]);
          multi_queue.erase(client_fd);
          exec_multi_queue(replys, client_fd);
        }
      } else {
        // sendReply({makeError("ERR EXEC without MULTI")}, client_fd);
        server_replies.emplace_back(makeError("ERR EXEC without MULTI"),
                                    client_fd);
      }
    } else if (command == "echo") {
      // items.erase(items.begin());
      //   sendReply(items, client_fd);
      server_replies.emplace_back(items[1], client_fd);
    } else if (command == "ping" && client_fd != _master_fd) {
      // cout << "in else if :"  << items[0].strVal << endl;

      RedisReply pong;
      pong.type = REPLY_STRING;
      pong.strVal = "PONG";
      //   sendReply({pong}, client_fd);
      server_replies.emplace_back(pong, client_fd);
    } else if (command == "set") {
      if (items.size() < 3)
        return {};
      string key = items[1].strVal;
      string value = items[2].strVal;
      if (items.size() == 5) {
        transform(items[3].strVal.begin(), items[3].strVal.end(),
                       command.begin(), ::tolower);
        if (items[3].strVal == "px") {
          key_elapsed_time_dbs[cur_db].insert_or_assign(
              key, currentTimeMillis() + stoi(items[4].strVal));
        }
      }
      // store[key] = value;
      kvs[cur_db].insert_or_assign(key, value);
      // cout << "is_master = " << is_master << " SET " << key << " " <<
      // value << endl;
      if (is_master) {
        // sendReply({makeString("OK")}, client_fd);
        server_replies.emplace_back(makeString("OK"), client_fd);
        for (int fd : slave_fds) {
          //   sendReply({reply}, fd);
          server_replies.emplace_back(reply, fd);
          // cout << "fd " << fd << " Send Slave:" << " KEY " << key << "
          // VALUE " << value << endl;
        }
        processed_bytes += reply.len;
      }
    } else if (command == "get") {
      if (items.size() < 2)
        return {};
      string key = items[1].strVal;
      // cout << "is_master = " << is_master << " GET " << key <<
      // endl;
      if (key_elapsed_time_dbs[cur_db].count(key)) {
        if (key_elapsed_time_dbs[cur_db][key] <= currentTimeMillis()) {
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
      //   sendReply({result}, client_fd);
      server_replies.emplace_back(result, client_fd);
    } else if (command == "config") {
      command = items[1].strVal;
      transform(command.begin(), command.end(), command.begin(),
                     ::tolower);
      if (command == "get") {
        auto response = makeArray({makeBulk("dir"), makeBulk(metadata["dir"])});
        // sendReply({response}, client_fd);
        server_replies.emplace_back(response, client_fd);
      }
    } else if (command == "keys") {
      string pattern = items[1].strVal;
      RedisReply reply;
      reply.type = RedisReplyType::REPLY_ARRAY;
      for (const auto &[key, _] : kvs[cur_db]) {
        if (matchPattern(pattern, key)) {
          reply.elements.emplace_back(std::move(makeBulk(key)));
        }
      }
      //   sendReply({reply}, client_fd);
      server_replies.emplace_back(reply, client_fd);
    } else if (command == "info") {
      string &arg = items[1].strVal;
      transform(arg.begin(), arg.end(), arg.begin(), ::tolower);
      if (arg == "replication") {
        ostringstream oss;
        if (is_master) {
          oss << "role:master\n";
          oss << "master_replid:" << metadata["master_replid"] << "\n";
          oss << "master_repl_offset:" << metadata["master_repl_offset"]
              << "\n";
        } else {
          oss << "role:slave\n";
        }
        // sendReply({makeBulk(oss.str())}, client_fd);
        server_replies.emplace_back(makeBulk(oss.str()), client_fd);
      }
    } else if (command == "replconf") {
      if (items.size() > 1) {
        string &arg = items[1].strVal;
        transform(arg.begin(), arg.end(), arg.begin(), ::tolower);
        if (arg == "getack") {
          //   sendReply({makeArray({makeBulk("REPLCONF"), makeBulk("ACK"),
          //                         makeBulk(to_string(processed_bytes))})},
          // client_fd);
          server_replies.emplace_back(
              makeArray({makeBulk("REPLCONF"), makeBulk("ACK"),
                         makeBulk(to_string(processed_bytes))}),
              client_fd);
        } else if (arg == "ack") {
          // cout << "REPLCONF ACK" << items[2].strVal << endl;
          if (wait_timer_event != nullptr) {
            if (wait_timer_event->ack_fds.count(client_fd) == 0) {
              int64_t offset = stoll(items[2].strVal);
              // cout << "fd = " << client_fd << " offset = " << offset
                        // << endl;
              slave_offsets[client_fd] = offset;
              if (offset >= processed_bytes) {
                wait_timer_event->ack_fds.insert(client_fd);
                wait_timer_event->inacks++;
              }
            }
          }
        } else {
          //   sendReply({makeString("OK")}, client_fd);
          server_replies.emplace_back(makeString("OK"), client_fd);
        }
      } else {
        // sendReply({makeString("OK")}, client_fd);
        server_replies.emplace_back(makeString("OK"), client_fd);
      }
    } else if (command == "psync") {
      sendReply({makeString("FULLRESYNC " + metadata["master_replid"] + " " +
                            metadata["master_repl_offset"])},
                client_fd);
      const string empty_rdb =
          "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73"
          "\x2d\x76\x65"
          "\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62"
          "\x69\x74\x73"
          "\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08"
          "\x75\x73\x65"
          "\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66\x2d"
          "\x62\x61\x73"
          "\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2";
      string content =
          "$" + to_string(empty_rdb.size()) + "\r\n" + empty_rdb;
      if (send(client_fd, content.c_str(), content.size(), 0) !=
          content.size()) {
        throw runtime_error("send RDB failed");
      }
      slave_fds.emplace_back(client_fd);
      slave_offsets.insert(make_pair(client_fd, 0));
      // cout << "slave client fd = " << client_fd << endl;
      // cout << "slave_fds.size() = " << slave_fds.size() << endl;
    } else if (command == "wait") {
      if (items.size() < 3)
        return {};
      int numreplicas = stoi(items[1].strVal);
      int timeout = stoi(items[2].strVal);

      // Check if there are any pending write operations
      bool has_pending_operations = false;
      for (int fd : slave_fds) {
        // cout << "slave_offsets[fd] = " << slave_offsets[fd]
        //           << " processed_bytes = " << processed_bytes << endl;
        if (slave_offsets[fd] < processed_bytes) {
          has_pending_operations = true;
          break;
        }
      }
      // cout << "has_pending_operations = " << has_pending_operations
      //           << endl;

      // If no pending operations, return immediately with 0
      if (!has_pending_operations) {
        // cout << "NO PENDING OPERATIONS" << endl;
        // sendReply({makeInterger(slave_fds.size())}, client_fd);
        server_replies.emplace_back(makeInterger(slave_fds.size()), client_fd);
        goto end;
      }

      // Send GETACK to all replicas to check their current offset
      for (int fd : slave_fds) {
        // cout << "SEND GETACK TO " << fd << endl;
        // sendReply({makeArray({makeBulk("REPLCONF"), makeBulk("GETACK"),
        //                       makeBulk("*")})},
        //           fd);
        server_replies.emplace_back(
            makeArray(
                {makeBulk("REPLCONF"), makeBulk("GETACK"), makeBulk("*")}),
            fd);
      }

      int timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
      itimerspec it = {
          .it_interval = {0, 0},
          .it_value = {timeout / 1000, (timeout % 1000) * 1000000}};
      timerfd_settime(timerfd, 0, &it, nullptr);
      epoll_event ev;
      ev.events = EPOLLIN;
      ev.data.fd = timerfd;
      wait_timer_event = new RedisWaitEvent(timerfd, client_fd, numreplicas,
                                            chrono::milliseconds(timeout));
      wait_timer_event->on_finish = [this](int client_fd) {
        sendReply({makeInterger(wait_timer_event->inacks)}, client_fd);
      };
      epoll_ctl(epoll_fd, EPOLL_CTL_ADD, timerfd, &ev);
    } else if (command == "type") {
      if (items.size() < 2)
        return {};
      string key = items[1].strVal;
      if (kvs[cur_db].count(key)) {
        // sendReply({makeString("string")}, client_fd);
        server_replies.emplace_back(makeString("string"), client_fd);
      } else if (streams.find(key) != streams.end()) {
        // sendReply({makeString("stream")}, client_fd);
        server_replies.emplace_back(makeString("stream"), client_fd);
      } else {
        // sendReply({makeString("none")}, client_fd);
        server_replies.emplace_back(makeString("none"), client_fd);
      }
    } else if (command == "xadd") {
      if (items.size() < 3)
        return {};
      string stream_key = items[1].strVal;
      string id = items[2].strVal;
      if (kvs[cur_db].count(stream_key) != 0) {
        throw runtime_error("stream key already exists in kvs");
      }
      string last_timestamp =
          streams[stream_key].getLastMillisecondsTime();
      string last_seqno = streams[stream_key].getLastSeqno();
      if (id == "*") {
        string timestamp = to_string(currentTimeMillis());
        id = timestamp + "-*";
      }
      string::size_type pos = id.find('-');
      string timestamp = id.substr(0, pos);
      string seqno = id.substr(pos + 1);
      if (seqno == "0" && timestamp == "0") {
        // sendReply({makeError(
        //               "ERR The ID specified in XADD must be greater than
        //               0-0")},
        //           client_fd);
        server_replies.emplace_back(
            makeError("ERR The ID specified in XADD must be greater than 0-0"),
            client_fd);
        goto end;
      }
      if (timestamp < last_timestamp ||
          (timestamp == last_timestamp &&
           (seqno != "*" && seqno <= last_seqno))) {
        // sendReply({makeError("ERR The ID specified in XADD is equal or "
        //                      "smaller than the target stream top item")},
        //           client_fd);
        server_replies.emplace_back(
            makeError("ERR The ID specified in XADD is equal or "
                      "smaller than the target stream top item"),
            client_fd);
        goto end;
      }

      if (timestamp == last_timestamp) {
        if (seqno == "*") {
          int next_seqno = stoi(last_seqno) + 1;
          id.pop_back();
          id += to_string(next_seqno);
        }
      } else {
        if (seqno == "*") {
          int next_seqno = timestamp == "0" ? 1 : 0;
          id.pop_back();
          id += to_string(next_seqno);
        }
      }
      vector<pair<string, string>> key_value;
      for (int i = 3; i < items.size(); i += 2) {
        key_value.emplace_back(
            make_pair(items[i].strVal, items[i + 1].strVal));
      }
      streams[stream_key].insert(id, key_value);
      check_xread_block_event(stream_key, id);
      // sendReply({makeBulk(id)}, client_fd);
      server_replies.emplace_back(makeBulk(id), client_fd);
    } else if (command == "xrange") {
      string stream_key = items[1].strVal;
      string start = items[2].strVal;
      string end = items[3].strVal;
      bool is_start = (start == "-") ? true : false;
      bool is_end = (end == "+") ? true : false;
      if (start.find("-") == string::npos) {
        start += "-0";
      }
      if (not is_end && end.find("-") == string::npos) {
        uint64_t end_no = stoll(end);
        end_no++;
        end = to_string(end_no) + "-0";
      }

      auto results = streams[stream_key].xrange(start, end, is_start, is_end);
      vector<RedisReply> replies;
      for (auto result : results) {
        vector<RedisReply> reply;
        reply.emplace_back(makeString(result.entry_id));
        vector<RedisReply> entries;
        for (const auto &[field, value] : result.fields) {
          entries.emplace_back(makeString(field));
          entries.emplace_back(makeString(value));
        }
        reply.emplace_back(makeArray(entries));
        replies.emplace_back(makeArray(reply));
      }
      // sendReply({makeArray(replies)}, client_fd);
      server_replies.emplace_back(makeArray(replies), client_fd);
    } else if (command == "xread") {
      if (items[1].strVal == "streams") {
        vector<RedisReply> streams_replies;
        int num_args = static_cast<int>((items.size() - 2) / 2);
        for (int i = 0; i < num_args; ++i) {
          int key_index = 2 + i;
          int id_index = 2 + num_args + i;
          string stream_key = items[key_index].strVal;
          string start_id = items[id_index].strVal;
          vector<RedisReply> single_stream_reply =
              AssembleStreamResults(client_fd, stream_key, start_id);
          streams_replies.emplace_back(makeArray(single_stream_reply));
        }

        // sendReply({makeArray(streams_replies)}, client_fd);
        server_replies.emplace_back(makeArray(streams_replies), client_fd);
      } else if (items[1].strVal == "block") {
        string stream_key = items[4].strVal;
        string start_id = items[5].strVal;
        if (start_id == "$") {
          start_id = streams[stream_key].getLastMillisecondsTime() + "-" +
                     streams[stream_key].getLastSeqno();
        }
        if (not streams[stream_key].findUpperId(start_id)) {
          int timeout = stoi(items[2].strVal);
          int timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
          itimerspec it = {
              .it_interval = {0, 0},
              .it_value = {timeout / 1000, (timeout % 1000) * 1000000}};
          timerfd_settime(timerfd, 0, &it, nullptr);
          epoll_event ev;
          ev.events = EPOLLIN;
          ev.data.fd = timerfd;
          xread_block_timer_event =
              new RedisXreadBlockEvent(timerfd, client_fd, stream_key, start_id,
                                       chrono::milliseconds(timeout));
          xread_block_timer_event->on_finish = [this](int client_fd) {
            sendReply({makeNIL()}, client_fd);
          };
          epoll_ctl(epoll_fd, EPOLL_CTL_ADD, timerfd, &ev);
        } else {
          vector<RedisReply> single_stream_reply =
              AssembleStreamResults(client_fd, stream_key, start_id);
          // sendReply({makeArray({makeArray(single_stream_reply)})},
          // client_fd);
          server_replies.emplace_back(
              makeArray({makeArray(single_stream_reply)}), client_fd);
        }
      }
    } else if (command == "incr") {
      string key = items[1].strVal;
      if (kvs[cur_db].find(key) != kvs[cur_db].end()) {
        if (not isNumber(kvs[cur_db][key])) {
          // sendReply({makeError("ERR value is not an integer or out of
          // range")},
          //           client_fd);
          server_replies.emplace_back(
              makeError("ERR value is not an integer or out of range"),
              client_fd);
          goto end;
        }
        int val = stoi(kvs[cur_db][key]);
        val++;
        kvs[cur_db][key] = to_string(val);
        // sendReply({makeInterger(val)}, client_fd);
        server_replies.emplace_back(makeInterger(val), client_fd);
      } else {
        kvs[cur_db][key] = to_string(1);
        // sendReply({makeInterger(1)}, client_fd);
        server_replies.emplace_back(makeInterger(1), client_fd);
      }
    } else if (command == "multi") {
      multi_queue[client_fd] = {};
      // sendReply({makeString("OK")}, client_fd);
      server_replies.emplace_back(makeString("OK"), client_fd);
    } else if (command == "rpush") {
      string key = items[1].strVal;
      for (int i = 2; i < items.size(); i++) {
        string value = items[i].strVal;
        key_lists[key].emplace_back(std::move(value));
      }
      server_replies.emplace_back(makeInterger(key_lists[key].size()),
                                  client_fd);
      check_blpop_event(key);
    } else if (command == "lrange") {
      string key = items[1].strVal;
      if (key_lists.find(key) != key_lists.end()) {
        int start = stoi(items[2].strVal);
        int end = stoi(items[3].strVal);
        if (start < 0)
          start += key_lists[key].size();
        if (end < 0)
          end += key_lists[key].size();
        start = max(start, 0);
        end = min(end, static_cast<int>(key_lists[key].size() - 1));

        vector<RedisReply> replies;
        for (; start <= end; start++) {
          replies.emplace_back(makeBulk(key_lists[key][start]));
        }
        // sendReply({makeArray(replies)}, client_fd);
        server_replies.emplace_back(makeArray(replies), client_fd);
      } else {
        server_replies.emplace_back(makeArray({}), client_fd);
      }
    } else if (command == "lpush") {
      string key = items[1].strVal;
      for (int i = 2; i < items.size(); i++) {
        string value = items[i].strVal;
        key_lists[key].emplace_front(std::move(value));
      }
      server_replies.emplace_back(makeInterger(key_lists[key].size()),
                                  client_fd);
      check_blpop_event(key);
    } else if (command == "llen") {
      string key = items[1].strVal;
      if (key_lists.find(key) != key_lists.end()) {
        server_replies.emplace_back(makeInterger(key_lists[key].size()),
                                    client_fd);
      } else {
        server_replies.emplace_back(makeInterger(0), client_fd);
      }
    } else if (command == "lpop") {
      int sz = 1;
      string key = items[1].strVal;
      if (items.size() == 3) {
        sz = stoi(items[2].strVal);
        sz = min(sz, static_cast<int>(key_lists[key].size()));
      }
      if (sz == 1) {
        server_replies.emplace_back(makeBulk(key_lists[key].front()),
                                    client_fd);
        key_lists[key].pop_front();
      } else if (key_lists.find(key) != key_lists.end()) {
        vector<RedisReply> replies;
        for (int i = 0; i < sz; i++) {
          replies.emplace_back(makeBulk(key_lists[key].front()));
          key_lists[key].pop_front();
        }
        server_replies.emplace_back(makeArray(replies), client_fd);
        // server_replies.emplace_back(makeBulk(key_lists[key].front()),
        // client_fd); key_lists[key].pop_front();
      } else {
        server_replies.emplace_back(makeBulk(""), client_fd);
      }
    } else if (command == "blpop") {
      string key = items[1].strVal;
      int timeout = static_cast<int>(stof(items[2].strVal) * 1000);
      auto now = chrono::steady_clock::now();
      chrono::steady_clock::time_point expire_at = now + chrono::milliseconds(timeout);
      auto* ev = new RedisBlpopEvent{client_fd, key, expire_at};
      blpop_events_by_key[key].push_back(ev);
      if(timeout != 0) {
        blpop_events_by_time.insert({ev->expire_time, ev});
      }
    } else if(command == "subscribe") {
      string channel_name = items[1].strVal;
      channel_subscribers[channel_name].insert(client_fd);
      client_subscribe_channels[client_fd].insert(channel_name);
      // cout << "client fd = " << client_fd << " " << (client_subscribe_channels.find(client_fd) != client_subscribe_channels.end()) << endl;
      server_replies.emplace_back(makeArray({makeBulk("subscribe"), makeBulk(channel_name), makeInterger(client_subscribe_channels[client_fd].size())}), client_fd);
    } else if(command == "publish") {
      string channel_name = items[1].strVal;
      string content = items[2].strVal;
      int size = 0;
      if(channel_subscribers.find(channel_name) != channel_subscribers.end()) {
        size = channel_subscribers[channel_name].size();
        for(int cli_fd : channel_subscribers[channel_name]) {
          server_replies.emplace_back(makeArray({makeBulk("message"), makeBulk(channel_name), makeBulk(content)}), cli_fd);
        }
      }
      server_replies.emplace_back(makeInterger(size), client_fd);
    } else if(command == "unsubscribe") {
      string channel_name = items[1].strVal;
      if(client_subscribe_channels[client_fd].find(channel_name) != client_subscribe_channels[client_fd].end()) {
        client_subscribe_channels[client_fd].erase(channel_name);
        channel_subscribers[channel_name].erase(client_fd);
      }
      server_replies.emplace_back(makeArray({makeBulk("unsubscribe"), makeBulk(channel_name), makeInterger(client_subscribe_channels[client_fd].size())}), client_fd);
    } else if(command == "zadd") {
      string set_name = items[1].strVal;
      int ret_val = 0;
      string member = items[3].strVal;
      char *endptr;
      double score = strtod(items[2].strVal.c_str(), &endptr);
      if(zsets.find(set_name) != zsets.end()) {
        auto &set = zsets[set_name];
        if(set.member_score.find(member) != set.member_score.end()) {
          ret_val = 0;
        } else {
          ret_val = 1;
        }
        set.member_score.insert_or_assign(member, score);
        set.score_member.insert(make_pair(score, member));
      } else {
        zsets.emplace(set_name, SortedSet{});
        auto &set = zsets[set_name];
        set.member_score.insert_or_assign(member, score);
        set.score_member.insert(make_pair(score, member));
        ret_val = 1;
      }
      server_replies.emplace_back(makeInterger(ret_val), client_fd);
    } else if(command == "zrank") {
      string set_name = items[1].strVal;
      int ret_val = -1;
      string member = items[2].strVal;
      if(zsets.find(set_name) != zsets.end()) {
        auto &set = zsets[set_name];
        if(set.member_score.find(member) != set.member_score.end()) {
          auto it = set.score_member.find(make_pair(set.member_score[member], member));
          ret_val = distance(set.score_member.begin(), it);
        }
      }
      if(ret_val == -1) {
      server_replies.emplace_back(makeNIL(), client_fd);
      } else {
      server_replies.emplace_back(makeInterger(ret_val), client_fd);
      }
    } else if(command == "zrange") {
      string set_name = items[1].strVal;
      int start = items[2].intVal, end = items[3].intVal;
      cout << set_name << " " << start << " " << end << endl;
      if(zsets.find(set_name) == zsets.end()) {
        server_replies.emplace_back(makeArray({}), client_fd);
      } else {
        auto &score_member = zsets[set_name].score_member;
        if (start < 0)
          start += score_member.size();
        if (end < 0)
          end += score_member.size();
        start = max(start, 0);
        end = min(end, static_cast<int>(score_member.size() - 1));
        int i = 0;
        vector<RedisReply> reply;
        for(auto iter = score_member.begin(); iter != score_member.end() && i < end; iter++, i++) {
          if(i >= start) {
            reply.emplace_back(makeBulk(iter->second));
          }
        }
        server_replies.emplace_back(makeArray(reply), client_fd);
      }
    } else if(command == "zcard") {
      string set_name = items[1].strVal;
      if(zsets.find(set_name) == zsets.end()) {
        server_replies.emplace_back(makeInterger(0), client_fd);
      } else {
        server_replies.emplace_back(makeInterger(zsets[set_name].member_score.size()), client_fd);
      }
    } else if(command == "zscore") {

    }
  end:
    if (client_fd == _master_fd) {
      processed_bytes += reply.len;
    }
    return server_replies;
  }

  string formatReply(const RedisReply &r) {
    ostringstream oss;
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
      for (const auto &sub : r.elements) {
        oss << formatReply({sub});
      }
      break;
    }
    return oss.str();
  }

  void check_blpop_event(string key) {
    if(blpop_events_by_key.find(key) != blpop_events_by_key.end()) {
      auto *ev = blpop_events_by_key[key].front();
      blpop_events_by_time.erase({ev->expire_time, ev});
      string value = key_lists[key].front();
      key_lists[key].pop_front();
      sendReply(makeArray({makeString(key), makeString(value)}), ev->client_fd);
      auto& vec = blpop_events_by_key[key];
      vec.erase(remove(vec.begin(), vec.end(), ev), vec.end());
      delete ev;
    }
  }

  void handle_blpop_timeout(int client_fd) {
    sendReply(makeNIL(), client_fd);
  }

  RedisReply makeBulk(const string &s) {
    RedisReply r;
    r.type = REPLY_BULK;
    r.strVal = s;
    return r;
  }

  RedisReply makeNIL() {
    RedisReply r;
    r.type = REPLY_NIL;
    return r;
  }

  RedisReply makeInterger(const int val) {
    RedisReply r;
    r.type = REPLY_INTEGER;
    // r.strVal = s;
    r.intVal = val;
    return r;
  }

  RedisReply makeArray(const vector<RedisReply> &elements) {
    RedisReply r;
    r.type = REPLY_ARRAY;
    r.elements = elements;
    return r;
  }

  RedisReply makeString(const string &s) {
    RedisReply r;
    r.type = REPLY_STRING;
    r.strVal = s;
    return r;
  }

  RedisReply makeError(const string &s) {
    RedisReply r;
    r.type = REPLY_ERROR;
    r.strVal = s;
    return r;
  }
};

#endif // REDIS_HH
