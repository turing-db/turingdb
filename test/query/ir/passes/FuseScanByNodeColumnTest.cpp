#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryConfig.h"
#include "QueryState.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "ID.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

#include "BioAssert.h"

using namespace db;
using namespace turing::test;

namespace {

class NullSink : public NLOutputSink {
public:
    void declareOutput(std::span<const std::string_view> names,
                       std::span<const Column* const> chunks) override {}
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {}
};

// Remy is a Person, Ghosts an Interest, and the third ID names no node of simpledb. They
// score 0, 9 and 36 against (1, 0, 0, 0).
constexpr std::string_view searchThree = "VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) ";

constexpr uint64_t danglingNodeID = 1000;

}

// A MATCH whose node is equated to the nodes a VECTOR SEARCH yielded reads those nodes
// rather than scanning, and keeps the rows a scan would have kept.
class FuseScanByNodeColumnTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            Graph* graph = system.createGraph(_graphName);
            SimpleGraph::createSimpleGraph(graph);

            _remy = SimpleGraph::findNodeID(graph, "Remy");
            _ghosts = SimpleGraph::findNodeID(graph, "Ghosts");
        }

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        loadPeopleVectors();
    }

protected:
    void runQuery(std::string_view query, QueryStatus& status, NLOutputSink& sink, ChangeID change = ChangeID::head()) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              change,
                              &sink);
    }

    ChangeID openChange() {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto opened = system.newChange(_graphName);
        bioassert(opened, "Failed to open a change");

        return opened.value()->id();
    }

    void runWrite(std::string_view query) {
        const ChangeID change = openChange();

        QueryStatus status;
        NullSink sink;
        runQuery(query, status, sink, change);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        const QueryState submitState(_graphName, &_env->getMem(), &_env->getCompilerContext(), &_queryConfig, nullptr, CommitHash::head(), change);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << submitStatus.getError();
    }

    void loadPeopleVectors() {
        const fs::Path path = _env->getConfig().getDataDir() / "people.csv";

        std::ofstream file(path.get());
        file << _remy.getValue() << ",1,0,0,0\n"
             << _ghosts.getValue() << ",4,0,0,0\n"
             << danglingNodeID << ",7,0,0,0\n";
        file.close();

        QueryStatus createStatus;
        NullSink createSink;
        runQuery("CREATE VECTOR INDEX people WITH DIMENSION 4 METRIC EUCLID", createStatus, createSink);
        ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

        QueryStatus loadStatus;
        NullSink loadSink;
        runQuery("LOAD VECTOR FROM \"people.csv\" IN people", loadStatus, loadSink);
        ASSERT_TRUE(loadStatus.isOk()) << loadStatus.getError();
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, status, sink);

        ASSERT_TRUE(status.isOk()) << status.getError();

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::ranges::sort(sortedExpected);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sortedExpected);
    }

    const std::string _graphName = "simpledb";
    NodeID _remy;
    NodeID _ghosts;
    QueryConfig _queryConfig;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(FuseScanByNodeColumnTest, labelledMatchKeepsTheNeighboursCarryingTheLabel) {
    expectRows(std::string(searchThree) + "YIELD ids MATCH (n:Person) WHERE n = ids RETURN n.name",
               {{"Remy"}});

    expectRows(std::string(searchThree) + "YIELD ids MATCH (n:Interest) WHERE n = ids RETURN n.name",
               {{"Ghosts"}});
}

TEST_F(FuseScanByNodeColumnTest, unlabelledMatchDropsTheIDNamingNoNode) {
    expectRows(std::string(searchThree) + "YIELD ids MATCH (n) WHERE n = ids RETURN n.name",
               {{"Remy"}, {"Ghosts"}});
}

TEST_F(FuseScanByNodeColumnTest, matchCarriesTheScoreOfEachNeighbour) {
    expectRows(std::string(searchThree) + "YIELD ids, score MATCH (n) WHERE n = ids RETURN n.name, score",
               {{"Remy", "0"}, {"Ghosts", "9"}});
}

TEST_F(FuseScanByNodeColumnTest, otherConjunctsStillCutTheRows) {
    expectRows(std::string(searchThree) + "YIELD ids, score MATCH (n) WHERE n = ids AND score > 1 RETURN n.name",
               {{"Ghosts"}});

    expectRows(std::string(searchThree) + "YIELD ids MATCH (n) WHERE n = ids AND n.name <> 'Ghosts' RETURN n.name",
               {{"Remy"}});
}

TEST_F(FuseScanByNodeColumnTest, inlinePropertyMapCutsTheNeighbours) {
    expectRows(std::string(searchThree) + "YIELD ids MATCH (n:Person {age: 32}) WHERE n = ids RETURN n.name",
               {{"Remy"}});

    expectRows(std::string(searchThree) + "YIELD ids MATCH (n {name: 'Ghosts'}) WHERE n = ids RETURN n.name",
               {{"Ghosts"}});

    expectRows(std::string(searchThree) + "YIELD ids MATCH (n:Person {age: 99}) WHERE n = ids RETURN n.name",
               {});
}

TEST_F(FuseScanByNodeColumnTest, matchDropsADeletedNeighbour) {
    runWrite("MATCH (n {name: 'Ghosts'}) DETACH DELETE n");

    expectRows(std::string(searchThree) + "YIELD ids MATCH (n) WHERE n = ids RETURN n.name",
               {{"Remy"}});
}

TEST_F(FuseScanByNodeColumnTest, matchOnAnEarlierClauseVariableKeepsItsLabelledNodes) {
    expectRows("MATCH (a:Interest) WITH a MATCH (n:Person) WHERE n = a RETURN n.name", {});

    expectRows("MATCH (a:Founder) WITH a MATCH (n:Person) WHERE n = a RETURN n.name",
               {{"Remy"}, {"Adam"}});
}

TEST_F(FuseScanByNodeColumnTest, matchDropsTheNullsOfAnOptionalMatch) {
    expectRows("OPTIONAL MATCH (a:Nope) MATCH (n) WHERE n = a RETURN n.name", {});
}
