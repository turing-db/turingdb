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

// The writes a query part after a WITH performs: one per row the barrier published, over
// the entities it published rather than the ones the match produced.
class WithWriteTest : public TuringTest {
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
        ASSERT_TRUE(submitStatus.isOk()) << "CHANGE SUBMIT failed";
    }

    // A write the engine turns away has to name what is wrong with the query, rather than
    // trip an assertion on the way down
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

// The SET runs over the one row the cut left, which is Adam: the first Person in name order
TEST_F(WithWriteTest, setsAPropertyOnTheRowsACutLeft) {
    applyWrite("MATCH (p:Person) WITH p ORDER BY p.name LIMIT 1 SET p.dob = '02/02'");

    expectRows("MATCH (p:Person) WITH p.name AS name, p.dob AS dob WHERE dob = '02/02' "
               "RETURN name",
               {{"Adam"}});
}

// Gym reaches the barrier three times and leaves it once, so the SET writes it once.
// Eighties was already unreal
TEST_F(WithWriteTest, setsAPropertyOnDedupedRows) {
    applyWrite("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH DISTINCT i WHERE i.name = 'Gym' "
               "SET i.isReal = false");

    expectRows("MATCH (i:Interest {isReal: false}) RETURN i.name",
               {{"Eighties"}, {"Gym"}});
}

// Only the interest more than two Persons reach survives the barrier's filter
TEST_F(WithWriteTest, setsAPropertyOnAGroupTheFilterKept) {
    applyWrite("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH i, count(p) AS fans WHERE fans > 2 "
               "SET i.isReal = false");

    expectRows("MATCH (i:Interest {isReal: false}) RETURN i.name",
               {{"Eighties"}, {"Gym"}});
}

// The barrier published the edge, and the DELETE reads that column: Adam stops knowing
// Remy well, and the edge the other way stays
TEST_F(WithWriteTest, deletesAnEdgeTheBarrierPublished) {
    applyWrite("MATCH (a:Person {name: 'Adam'})-[e:KNOWS_WELL]->(b) WITH e DELETE e");

    expectRows("MATCH (a:Person)-[e:KNOWS_WELL]->(b) RETURN e.name", {{"Remy -> Adam"}});
}

TEST_F(WithWriteTest, detachDeletesANodeTheBarrierPublished) {
    applyWrite("MATCH (p:Person {name: 'Martina'}) WITH p DETACH DELETE p");

    expectCounts("MATCH (p:Person) RETURN count(*)", {7});
}

// Martina is interested in Cooking, so deleting her node alone would leave that edge
// dangling
TEST_F(WithWriteTest, rejectsDeletingAConnectedNodeTheBarrierPublished) {
    expectWriteRejected("MATCH (p:Person {name: 'Martina'}) WITH p DELETE p",
                        QueryStatus::Status::EXEC_ERROR);
}

// One node per row the barrier published, each carrying the value that row holds
TEST_F(WithWriteTest, createsANodePerRowFromAPublishedValue) {
    applyWrite("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 2 "
               "CREATE (:Tag {name: name})");

    expectRows("MATCH (t:Tag) RETURN t.name", {{"Adam"}, {"Cyrus"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
