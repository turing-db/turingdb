#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class MergeListPropertyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
    }

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        changeID = res.value()->id();
    }

    void submit(const ChangeID& changeID) {
        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_env->getCompilerContext(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << "CHANGE SUBMIT failed";
    }

    void write(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), changeID, &sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        submit(changeID);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(MergeListPropertyTest, createsANodeMergedOnAList) {
    write("MERGE (n:Tagged {attrs: [1, 2]})");
    expectRows("MATCH (n:Tagged) WHERE n.attrs = [1, 2] RETURN count(n)", {{"1"}});
}

TEST_F(MergeListPropertyTest, matchesANodeHoldingTheList) {
    write("CREATE (:Tagged {attrs: [1, 2]})");
    write("MERGE (n:Tagged {attrs: [1, 2]})");
    expectRows("MATCH (n:Tagged) RETURN count(n)", {{"1"}});
}

TEST_F(MergeListPropertyTest, createsANodeWhereTheListDiffers) {
    write("CREATE (:Tagged {attrs: [1, 2]})");
    write("MERGE (n:Tagged {attrs: [2, 1]})");
    expectRows("MATCH (n:Tagged) RETURN count(n)", {{"2"}});
}

TEST_F(MergeListPropertyTest, matchesAnIntegerListAgainstEqualFloats) {
    write("CREATE (:Tagged {attrs: [1, 2]})");
    write("MERGE (n:Tagged {attrs: [1.0, 2.0]})");
    expectRows("MATCH (n:Tagged) RETURN count(n)", {{"1"}});
}

TEST_F(MergeListPropertyTest, matchesAListHoldingANull) {
    write("MERGE (n:Tagged {attrs: [1, null]})");
    write("MERGE (n:Tagged {attrs: [1, null]})");
    expectRows("MATCH (n:Tagged) RETURN count(n)", {{"1"}});
}

TEST_F(MergeListPropertyTest, mergesAListBuiltPerRow) {
    write("UNWIND [1, 2, 1] AS i MERGE (n:Tagged {attrs: [i, 0]})");
    write("UNWIND [2, 3] AS i MERGE (n:Tagged {attrs: [i, 0]})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"1, 0"}, {"2, 0"}, {"3, 0"}});
}

TEST_F(MergeListPropertyTest, mergesAnUnwoundList) {
    write("UNWIND [[1, 0], [2, 0], [1, 0]] AS l MERGE (n:Tagged {attrs: l})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"1, 0"}, {"2, 0"}});
}

TEST_F(MergeListPropertyTest, mergesAListReadFromAProperty) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.attrs = [1, 2]");
    write("MATCH (p:Person {name: 'Remy'}) MERGE (n:Tagged {attrs: p.attrs})");
    write("MATCH (p:Person {name: 'Remy'}) MERGE (n:Tagged {attrs: p.attrs})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"1, 2"}});
}

TEST_F(MergeListPropertyTest, mergesAnEdgeOnAList) {
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) MERGE (a)-[:TAGGED {attrs: [1, 0]}]->(b)");
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) MERGE (a)-[:TAGGED {attrs: [1, 0]}]->(b)");
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) MERGE (a)-[:TAGGED {attrs: [2, 0]}]->(b)");
    expectRows("MATCH ()-[e:TAGGED]->() RETURN e.attrs", {{"1, 0"}, {"2, 0"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
