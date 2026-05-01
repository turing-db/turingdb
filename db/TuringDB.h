#pragma once

#include <string_view>

#include "QueryConfig.h"
#include "QueryState.h"
#include "QueryStatus.h"

namespace db {

class TuringConfig;
class SystemManager;

class TuringDB {
public:
    TuringDB(const TuringConfig* config);
    ~TuringDB();

    void init();

    QueryStatus query(std::string_view query, const QueryState& state);

    const QueryConfig& getDefaultQueryConfig() const { return _defaultQueryConfig; }

    SystemManager& getSystemManager() { return *_systemManager; }

private:
    const TuringConfig* _config {nullptr};
    QueryConfig _defaultQueryConfig;

    std::unique_ptr<SystemManager> _systemManager;
};

}
