#include "ProtoServerTest.h"

#include <gtest/gtest.h>

#include <chrono>

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringServer.h"

using namespace db;
using namespace turing::test;

ProtoServerTest::ProtoServerTest()
{
}

ProtoServerTest::~ProtoServerTest() {
}

void ProtoServerTest::initialize() {
    _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

    Graph* graph = nullptr;
    {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        system.createGraph("default");
        graph = system.createGraph("simpledb");
    }

    SimpleGraph::createSimpleGraph(graph);

    _port = reserveFreePort();

    _serverConfig.setAddress("127.0.0.1");
    _serverConfig.setPort(_port);
    _serverConfig.setWorkerCount(1);
    _serverConfig.setMaxConnections(16);

    _server = std::make_unique<TuringServer>(_serverConfig, _env->getDB());
    _server->start();

    ASSERT_TRUE(waitUntilListening(_port, std::chrono::milliseconds(2000)));
}

void ProtoServerTest::terminate() {
    if (_server) {
        _server->stop();
        _server->wait();
    }
}
