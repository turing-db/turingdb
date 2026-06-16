#include "AuthGate.h"

#include "HTTPParsingInfo.h"
#include "HTTPUtils.h"
#include "Authenticator.h"

using namespace db;

std::string_view db::extractBearerToken(std::string_view authorization) {
    constexpr std::string_view prefix = "bearer ";

    if (authorization.size() <= prefix.size()) {
        return {};
    }

    if (!net::http::equalsIgnoreCaseAscii(authorization.substr(0, prefix.size()), prefix)) {
        return {};
    }

    std::string_view token = authorization.substr(prefix.size());

    while (!token.empty() && (token.front() == ' ' || token.front() == '\t')) {
        token.remove_prefix(1);
    }

    while (!token.empty() && (token.back() == ' ' || token.back() == '\t' || token.back() == '\r')) {
        token.remove_suffix(1);
    }

    return token;
}

bool db::isRequestAuthorized(Authenticator* authenticator, const net::HTTP::Info& info) {
    const bool authDisabled = (authenticator == nullptr) || !authenticator->isEnabled();
    if (authDisabled) {
        return true;
    }

    const std::string_view token = extractBearerToken(info._authorization);
    if (token.empty()) {
        return false;
    }

    return authenticator->check(token);
}
