#pragma once

#include <string>
#include <stdint.h>

#include "TuringConfig.h"
#include "QueryResult.h"

namespace turing::db::client {

class TuringConfig;
class TuringClientImpl;

class TuringClient {
public:
    TuringClient();
    ~TuringClient();

    TuringConfig& getConfig() { return _config; }
    
    bool connect();
    QueryResult exec(const std::string& query);

private:
    TuringConfig _config;
    TuringClientImpl* _impl {nullptr};
};

}
