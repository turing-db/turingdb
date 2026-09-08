#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "dataframe/Dataframe.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A CREATE and a DELETE in one query, over the entities a MATCH bound.
class CreateThenDeleteTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        changeID = res.value()->id();
    }

    QueryStatus runWrite(std::string_view query, const ChangeID& changeID) {
        NullSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);

        return status;
    }

    void applyWrite(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     &callbacks,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "CHANGE SUBMIT failed: " << submitStatus.getError();
    }

    void expectWriteRejected(std::string_view query, QueryStatus::Status stage) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        EXPECT_EQ(status.getStatus(), stage)
            << "query: " << query
            << "\nstage: " << QueryStatusDescription::value(status.getStatus())
            << "\nerror: " << status.getError();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

// Both writes land: the Tag is created and Remy, who the MATCH bound, is gone with the
// edges he walked
TEST_F(CreateThenDeleteTest, createsANodeAndDetachDeletesAMatchedOne) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) CREATE (:Tag {name: 'x'}) DETACH DELETE p");

    expectRows("MATCH (t:Tag) RETURN t.name", {{"x"}});
    expectCounts("MATCH (p:Person) RETURN count(*)", {7});
}

// An edge a MATCH bound is deletable beside a CREATE the same way a node is
TEST_F(CreateThenDeleteTest, createsANodeAndDeletesAMatchedEdge) {
    applyWrite("MATCH (:Person {name: 'Adam'})-[e:INTERESTED_IN]->(:Interest {name: 'Bio'}) "
               "CREATE (:Tag {name: 'x'}) DELETE e");

    expectRows("MATCH (t:Tag) RETURN t.name", {{"x"}});
    expectRows("MATCH (:Person {name: 'Adam'})-[e:INTERESTED_IN]->(i) RETURN i.name",
               {{"Cooking"}});
}

// The undetached delete of a connected node is turned away for what is wrong with it -
// Remy's relationships - not for the CREATE standing beside it
TEST_F(CreateThenDeleteTest, rejectsDeletingAConnectedNodeBesideACreate) {
    expectWriteRejected("MATCH (p:Person {name: 'Remy'}) CREATE (:Tag {name: 'x'}) DELETE p",
                        QueryStatus::Status::EXEC_ERROR);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
