#include "Authenticator.h"

#include <stddef.h>

using namespace db;

namespace {

// Length-independent (modulo the early size check) comparison, to avoid leaking
// the configured key through timing.
bool constantTimeEquals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) {
        return false;
    }

    unsigned char diff = 0;
    for (size_t i = 0; i < a.size(); i++) {
        diff |= static_cast<unsigned char>(a[i]) ^ static_cast<unsigned char>(b[i]);
    }

    return diff == 0;
}

}

Authenticator::Authenticator(const std::string& apiKey)
    : _apiKey(apiKey),
    _enabled(!apiKey.empty())
{
}

Authenticator::~Authenticator() {
}

bool Authenticator::check(std::string_view presentedKey) const {
    if (!_enabled) {
        return true;
    }

    if (presentedKey.empty()) {
        return false;
    }

    return constantTimeEquals(presentedKey, _apiKey);
}
