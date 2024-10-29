#pragma once

#include <string_view>
#include <vector>
#include <stdint.h>

#include "HTTP.h"

typedef void CURL;

namespace net::HTTP {

class HTTPClient {
public:
    struct ResponseBuffer {
        std::vector<uint8_t> _data;

        void clear() { _data.clear(); }

        std::string_view getString() const {
            return std::string_view((const char*)_data.data(), _data.size());
        }
    };

    HTTPClient();
    ~HTTPClient();

    HTTPClient(const HTTPClient&) = delete;
    HTTPClient(HTTPClient&&) = delete;
    HTTPClient& operator=(const HTTPClient&) = delete;
    HTTPClient& operator=(HTTPClient&&) = delete;

    net::HTTP::Status fetch(std::string_view url,
                            std::string_view reqData,
                            ResponseBuffer& resp);

private:
    CURL* _curl {nullptr};
};

}