#include "CallV3Test.h"

#include <gtest/gtest.h>

#include <span>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {}
};

}

CallV3Test::CallV3Test() {
}

CallV3Test::~CallV3Test() {
}

void CallV3Test::initialize() {
    _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

    SystemAccessor system = _env->getSystemManager().accessUnique();
    Graph* graph = system.createGraph(_graphName);
    SimpleGraph::createSimpleGraph(graph);

    _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
}

void CallV3Test::runQuery(std::string_view query, NLOutputSink& sink) {
    QueryStatus status;
    _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
    ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
}

void CallV3Test::runQueryExpectingError(std::string_view query, std::string_view reason) {
    NullSink sink;
    QueryStatus status;
    _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
    ASSERT_FALSE(status.isOk()) << "accepted: " << query;

    const std::string error = status.getError();
    EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
}

void CallV3Test::runWrite(std::string_view query) {
    ChangeID changeID;
    newChange(changeID);

    NullSink sink;
    QueryStatus status;
    _interpreter->execute(status, query, _graphName, CommitHash::head(), changeID, &_env->getMem(), &sink);
    ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

    submitChange(changeID);
}

void CallV3Test::runWriteExpectingError(std::string_view query, std::string_view reason) {
    ChangeID changeID;
    newChange(changeID);

    NullSink sink;
    QueryStatus status;
    _interpreter->execute(status, query, _graphName, CommitHash::head(), changeID, &_env->getMem(), &sink);
    ASSERT_FALSE(status.isOk()) << "accepted: " << query;

    const std::string error = status.getError();
    EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
}

// A query the MLIR engine does not run yet, applied through the legacy engine and committed
// so the head carries its effect.
void CallV3Test::runLegacyWrite(std::string_view query) {
    ChangeID changeID;
    newChange(changeID);

    QueryCallbacks callbacks;
    callbacks.setOnOutputData([](const Dataframe*) {});

    const QueryState writeState(_graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
    const QueryStatus writeStatus = _env->getDB().query(query, writeState);
    ASSERT_TRUE(writeStatus.isOk()) << query << ": " << writeStatus.getError();

    const QueryStatus commitStatus = _env->getDB().query("COMMIT", writeState);
    ASSERT_TRUE(commitStatus.isOk()) << commitStatus.getError();

    submitChange(changeID);
}

void CallV3Test::newChange(ChangeID& changeID) {
    SystemAccessor system = _env->getSystemManager().accessUnique();
    auto res = system.newChange(_graphName);
    ASSERT_TRUE(res);

    changeID = res.value()->id();
}

void CallV3Test::submitChange(ChangeID changeID) {
    QueryCallbacks callbacks;
    callbacks.setOnOutputData([](const Dataframe*) {});

    const QueryState submitState(_graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
    const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
    ASSERT_TRUE(submitStatus.isOk()) << submitStatus.getError();
}
