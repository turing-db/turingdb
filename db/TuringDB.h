#pragma once

#include <string_view>

#include "QueryConfig.h"
#include "QueryState.h"
#include "QueryStatus.h"

namespace db {

class TuringConfig;
class SystemManager;
class Authenticator;

class TuringDB {
public:
    TuringDB(const TuringConfig* config);
    TuringDB(const TuringConfig* config, const QueryConfig& defaultQueryConfig);
    ~TuringDB();

    void init();

    QueryStatus query(std::string_view query, const QueryState& state);

    const QueryConfig& getDefaultQueryConfig() const { return _defaultQueryConfig; }

    SystemManager& getSystemManager() { return *_systemManager; }

    // Non-owning. Null means authentication is disabled. Set by the server
    // startup code when launched with -auth-on.
    void setAuthenticator(Authenticator* authenticator) { _authenticator = authenticator; }
    Authenticator* getAuthenticator() { return _authenticator; }

private:
    QueryConfig _defaultQueryConfig;

    std::unique_ptr<SystemManager> _systemManager;
    Authenticator* _authenticator {nullptr};
};
}
