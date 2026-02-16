#pragma once
#include <string>
#include <vector>
#include <openssl/sha.h>
#include <iomanip>
#include <sstream>
#include <string>

struct UserInfo
{
    std::string username;
    bool nopass;
    std::vector<std::string> flags;
    std::vector<std::string> passwords;
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
