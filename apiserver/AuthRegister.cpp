#include "AuthRegister.h"

#include "APIServerConfig.h"

AuthRegister::AuthRegister(const APIServerConfig& config)
{
    setCredential(config.getDefaultAuthID(), config.getDefaultAuthSecret());
}

AuthRegister::~AuthRegister() {
}

bool AuthRegister::isValid(const AuthID& id, const AuthSecret& secret) const {
    const auto it = _register.find(id);
    if (it == _register.end()) {
        return false;
    }

    return it->second == secret;
}

bool AuthRegister::setCredential(const AuthID& id, const AuthSecret& secret) {
    if (id.empty() || secret.empty()) {
        return false;
    }

    std::lock_guard<std::mutex> guard(_mutex);
    _register[id] = secret;

    return true;
}
