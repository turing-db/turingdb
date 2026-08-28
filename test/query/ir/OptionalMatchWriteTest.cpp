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

// The writes a query part performs behind an OPTIONAL MATCH. Two families of column reach
// them: the ones the join carried, which every row it kept holds - the padded ones as much
// as the matched ones - and the pattern's own variables, which a row it missed holds null.
// A null entity is written nothing, so the second family must reach the matched rows alone.
class OptionalMatchWriteTest : public TuringTest {
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

    // Runs a writing query in its own change and submits it, so a following read sees it
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

    // A write the engine turns away has to name what is wrong with the query, at the
    // statement that is wrong - rather than stage it and trip an internal error at the
    // commit
    void expectWriteRejected(std::string_view query, QueryStatus::Status stage) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        const std::string& error = status.getError();

        EXPECT_EQ(status.getStatus(), stage)
            << "query: " << query
            << "\nstage: " << QueryStatusDescription::value(status.getStatus())
            << "\nerror: " << error;

        EXPECT_EQ(error.find("Unexpected exception"), std::string::npos)
            << "query: " << query << "\nerror: " << error;
        EXPECT_EQ(error.find("Internal Error"), std::string::npos)
            << "query: " << query << "\nerror: " << error;
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

// p is bound ahead of the join, so all eight Persons are written - only Remy and Adam walk
// a KNOWS_WELL edge, and the six the pattern missed carry their own p through
TEST_F(OptionalMatchWriteTest, setsAPropertyOnEveryRowTheJoinKept) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) SET p.dob = '02/02'");

    expectCounts("MATCH (p:Person {dob: '02/02'}) RETURN count(*)", {8});
}

// One node per row the join kept, carrying the value that row holds
TEST_F(OptionalMatchWriteTest, createsANodePerRowTheJoinKept) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "CREATE (:Tag {name: p.name})");

    expectRows("MATCH (t:Tag) RETURN t.name",
               {{"Remy"},
                {"Adam"},
                {"Maxime"},
                {"Luc"},
                {"Martina"},
                {"Suhas"},
                {"Cyrus"},
                {"Doruk"}});
}

// The two KNOWS_WELL edges between Persons are the ones the pattern matched; the one out of
// Ghosts, which no Person walks, stays
TEST_F(OptionalMatchWriteTest, deletesTheEdgesThePatternMatched) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[e:KNOWS_WELL]->(f) DELETE e");

    expectRows("MATCH ()-[e:KNOWS_WELL]->() RETURN e.name", {{"Ghosts -> Remy"}});
}

// f is Remy on Adam's row and Adam on Remy's, so the two of them go and six Persons remain
TEST_F(OptionalMatchWriteTest, detachDeletesTheNodesThePatternMatched) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) DETACH DELETE f");

    expectCounts("MATCH (p:Person) RETURN count(*)", {6});
}

// Doruk walks no KNOWS_WELL edge, so f is null on the one row the join kept and the delete
// has nothing to read
TEST_F(OptionalMatchWriteTest, deletesNothingWhenThePatternMatchedNoRow) {
    applyWrite("MATCH (p:Person {name: 'Doruk'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "DETACH DELETE f");

    expectCounts("MATCH (p:Person) RETURN count(*)", {8});
}

// Remy and Adam both carry further edges, so what the engine turns away is the undetached
// delete of a connected node - not the padded rows beside them
TEST_F(OptionalMatchWriteTest, rejectsDeletingAConnectedNodeThePatternMatched) {
    expectWriteRejected("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) DELETE f",
                        QueryStatus::Status::EXEC_ERROR);
}

TEST_F(OptionalMatchWriteTest, createsAnEdgeWhenEveryRowMatched) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "CREATE (p)-[:PROBE]->(f)");

    expectRows("MATCH (a:Person)-[:PROBE]->(b) RETURN a.name, b.name", {{"Remy", "Adam"}});
}

// f is the pattern's own variable: the two rows it matched hold Adam and Remy, and the six
// it missed hold a null, which is written nothing
TEST_F(OptionalMatchWriteTest, setsAPropertyOnTheRowsThePatternMatched) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) SET f.dob = '02/02'");

    expectRows("MATCH (p:Person {dob: '02/02'}) RETURN p.name", {{"Remy"}, {"Adam"}});
}

// Every row the join kept is a padded one, so the SET reads a column of nulls alone
TEST_F(OptionalMatchWriteTest, setsNoPropertyWhenThePatternMatchedNoRow) {
    applyWrite("MATCH (p:Person {name: 'Doruk'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "SET f.dob = '02/02'");

    expectCounts("MATCH (p:Person {dob: '02/02'}) RETURN count(*)", {0});
}

// An edge variable a row the pattern missed holds is null the way a node one is, so the
// same two edges are written and the third keeps the duration it had
TEST_F(OptionalMatchWriteTest, setsAPropertyOnTheEdgesThePatternMatched) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[e:KNOWS_WELL]->(f) SET e.duration = 99");

    expectRows("MATCH ()-[e:KNOWS_WELL]->() RETURN e.name, e.duration",
               {{"Remy -> Adam", "99"},
                {"Adam -> Remy", "99"},
                {"Ghosts -> Remy", "200"}});
}

// An edge cannot hang off a node that is null, so the six padded rows name no edge to
// create: the query is turned away where it is written, not staged and tripped over at the
// commit
TEST_F(OptionalMatchWriteTest, rejectsCreatingAnEdgeToANullEndpoint) {
    expectWriteRejected("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                        "CREATE (p)-[:PROBE]->(f)",
                        QueryStatus::Status::EXEC_ERROR);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
