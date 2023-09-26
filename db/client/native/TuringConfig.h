#pragma once

#include <string>

namespace turing {

class TuringConfig {
public:
    TuringConfig() = default;

    const std::string& getAddress() const { return _address; }
    short unsigned getPort() const { return _port; }

private:
    std::string _address {"127.0.0.1"};
    short unsigned _port {6666};
};

}
