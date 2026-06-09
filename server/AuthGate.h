#pragma once

#include <string_view>

namespace net::HTTP {
class Info;
}

namespace db {

class Authenticator;

// Returns the token from an Authorization header value of the form
// "Bearer <token>", or an empty view if the scheme does not match.
std::string_view extractBearerToken(std::string_view authorization);

// Returns true if the request may proceed: authentication is disabled, or a
// valid API key is presented in the Authorization header.
bool isRequestAuthorized(Authenticator* authenticator, const net::HTTP::Info& info);

}
