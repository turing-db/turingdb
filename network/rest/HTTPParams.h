#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <utility>

namespace net {

struct HTTPParams {
    std::string _uri;
    std::string_view _path;
    std::vector<std::pair<std::string_view, std::string_view>> _params;
};

}