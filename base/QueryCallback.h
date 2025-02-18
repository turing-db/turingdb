#pragma once

#include <functional>

namespace db {

class Block;

using QueryCallback = std::function<void(const Block& block)>;

}
