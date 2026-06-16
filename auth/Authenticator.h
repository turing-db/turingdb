#pragma once

#include <string>
#include <string_view>

namespace db {

// Verifies a single configured API key. Clients present the key directly in the
// Authorization header on every request ("Authorization: Bearer <key>").
//
// Owned by the server startup code and injected into TuringDB as a non-owning
// pointer. When no Authenticator is injected, authentication is disabled and
// every request is allowed through.
class Authenticator {
public:
    explicit Authenticator(const std::string& apiKey);
    ~Authenticator();

    Authenticator(const Authenticator&) = delete;
    Authenticator(Authenticator&&) = delete;
    Authenticator& operator=(const Authenticator&) = delete;
    Authenticator& operator=(Authenticator&&) = delete;

    bool isEnabled() const { return _enabled; }

    // Returns true if presentedKey matches the configured key (constant-time).
    bool check(std::string_view presentedKey) const;

private:
    std::string _apiKey;
    bool _enabled {false};
};

}
