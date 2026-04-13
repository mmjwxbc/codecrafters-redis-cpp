#pragma once
#include <string>
#include <vector>
#include <openssl/sha.h>
#include <iomanip>
#include <sstream>
#include <string>
#include <unordered_set>
#include <set>

struct UserInfo
{
    std::string username;
    bool nopass;
    std::vector<std::string> flags;
    std::vector<std::string> passwords;
    std::unordered_set<int> verified_client;
};

std::string sha256(const std::string &input)
{
    unsigned char hash[SHA256_DIGEST_LENGTH]; // 32 bytes
    SHA256(reinterpret_cast<const unsigned char *>(input.c_str()), input.size(), hash);

    std::ostringstream oss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i)
    {
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return oss.str();
}

struct client_watch_key
{
    std::set<std::string> watched_keys;

    bool in_transaction; // 是否已发送 MULTI
    bool dirty_cas;      // 关键：WATCH 的 Key 是否被修改 (CLIENT_DIRTY_CAS)

    client_watch_key() : in_transaction(false), dirty_cas(false) {}
};