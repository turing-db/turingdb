#pragma once

#include <string_view>

#include "SystemManager.h"
#include "QueryStatus.h"
#include "QueryCallback.h"

namespace db {

class LocalMemory;
class Block;

class TuringDB {
public:
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
