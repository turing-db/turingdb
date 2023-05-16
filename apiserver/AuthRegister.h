#pragma once

#include <string>
#include <unordered_map>
#include <mutex>

class APIServerConfig;

class AuthRegister {
public:
    using AuthID = std::string;
    using AuthSecret = std::string;

    AuthRegister(const APIServerConfig& config);
    ~AuthRegister();

    bool isValid(const AuthID& id, const AuthSecret& secret) const;

    bool setCredential(const AuthID& id, const AuthSecret& secret);

private:
    std::mutex _mutex;
    std::unordered_map<AuthID, AuthSecret> _register;
};
