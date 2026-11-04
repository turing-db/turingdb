#pragma once

#include <stdint.h>
#include <memory>

#include "DBServerConfig.h"
#include "RemoteTestUtils.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

namespace db {
class TuringServer;
}

namespace turing::test {

// A TuringServer speaking the binary protocol on a free port, serving simpledb
class ProtoServerTest : public TuringTest {
public:
    ProtoServerTest();
    ~ProtoServerTest() override;

protected:
    void initialize() override;
    void terminate() override;

    ProtoEnvScope _protoScope;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<db::TuringServer> _server;
    db::DBServerConfig _serverConfig;
    uint16_t _port {0};
};

}
