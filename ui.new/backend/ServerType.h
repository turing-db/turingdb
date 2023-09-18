#pragma once

#include <string>
#include <unordered_map>

namespace ui {

enum class ServerType : uint8_t {
    BIOSERVER = 0,
    FLASK,
    REACT,

    _SIZE
};

static inline const std::unordered_map<ui::ServerType, std::string> SERVER_NAMES = {
    {ServerType::BIOSERVER, "bioserver"},
    {ServerType::FLASK,     "Flask"    },
    {ServerType::REACT,     "React"    },
};
}
