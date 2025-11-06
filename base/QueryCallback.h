#pragma once

#include <functional>

namespace db {

class Block;
class QueryCommand;
class Dataframe;

using QueryCallback = std::function<void(const Block& block)>;
using QueryHeaderCallback = std::function<void(const QueryCommand* block)>;

using QueryCallbackV2 = std::function<void(const Dataframe* dataframe)>;

}
