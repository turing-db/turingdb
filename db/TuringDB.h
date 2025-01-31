#pragma once

#include <string_view>
#include <functional>

#include "SystemManager.h"
#include "QueryStatus.h"

namespace db {

class LocalMemory;
class Block;

class TuringDB {
public:
    using QueryCallback = std::function<void(const Block& block)>;

    TuringDB();
    ~TuringDB();

    QueryStatus query(std::string_view query,
                      std::string_view graphName,
                      LocalMemory* mem,
                      QueryCallback callback);

    QueryStatus query(std::string_view query,
                      std::string_view graphName,
                      LocalMemory* mem);

private:
    SystemManager _systemManager;
};

}
