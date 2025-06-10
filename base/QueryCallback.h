#pragma once

#include <functional>

namespace db {

class Block;
class QueryCommand;

using QueryCallback = std::function<void(const Block& block)>;
using QueryHeaderCallback = std::function<void(const QueryCommand* block)>;

}
