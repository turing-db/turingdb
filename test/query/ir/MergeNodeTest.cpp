#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

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

// MERGE of a node pattern: the rows it binds when the graph already holds the node, the
// node it writes when it does not, and the once-only writing a merge driven by repeated
// rows owes.
class MergeNodeTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        changeID = res.value()->id();
    }

    void submit(const ChangeID& changeID) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     &callbacks,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << "CHANGE SUBMIT failed";
    }

    // The rows a writing query emits, collected in its own change and then submitted, so
    // a following read sees what it wrote
    void writeRows(std::string_view query, Rows& rows) {
        ChangeID changeID;
        openChange(changeID);

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        sink.sortedRows(rows);

        submit(changeID);
    }

    void expectWriteRows(std::string_view query, const Rows& expected) {
        Rows actual;
        writeRows(query, actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectWriteRowCount(std::string_view query, size_t expected) {
        Rows actual;
        writeRows(query, actual);

        EXPECT_EQ(actual.size(), expected) << "query: " << query;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

// The graph has no Tag, so the merge writes one and the projection reads the value it
// wrote there
TEST_F(MergeNodeTest, writesANodeThePatternDoesNotFind) {
    expectWriteRows("MERGE (n:Tag {name: 'x'}) RETURN n.name", {{"x"}});

    expectRows("MATCH (t:Tag) RETURN t.name", {{"x"}});
}

// Remy is a Person of that name already, so the merge binds him and writes nothing
TEST_F(MergeNodeTest, bindsTheNodeThePatternFinds) {
    expectWriteRows("MERGE (n:Person {name: 'Remy'}) RETURN n.name", {{"Remy"}});

    expectRows("MATCH (p:Person) RETURN count(p)", {{"8"}});
}

// Merging the same pattern twice writes one node: the second statement binds what the
// first wrote
TEST_F(MergeNodeTest, writesOneNodeForTwoMergesOfOnePattern) {
    expectWriteRowCount("MERGE (a:Tag {name: 'x'}) MERGE (b:Tag {name: 'x'})", 0);

    expectRows("MATCH (t:Tag) RETURN t.name", {{"x"}});
}

// One node per distinct value the list unwinds, not one per row
TEST_F(MergeNodeTest, writesOneNodePerDistinctRow) {
    expectWriteRows("UNWIND [1, 1, 2] AS id MERGE (n:Tag {age: id}) RETURN n.age",
                    {{"1"}, {"1"}, {"2"}});

    expectRows("MATCH (t:Tag) RETURN t.age", {{"1"}, {"2"}});
}

// A pattern with no property constraint binds every node carrying the label - all eight
// Persons - and writes nothing
TEST_F(MergeNodeTest, bindsEveryNodeAPatternWithoutPropertiesFinds) {
    expectWriteRowCount("MERGE (p:Person) RETURN p", 8);
}

// The merge runs once per row the match produced, and each row's value keys its own
// lookup
TEST_F(MergeNodeTest, writesANodePerMatchedRow) {
    expectWriteRows("MATCH (p:Person) MERGE (t:Tag {name: p.name}) RETURN t.name",
                    {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"},
                     {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"8"}});
}

// The rows the match produced survive the merge's fan-out, so the projection pairs each
// Person with the tag merged for it
TEST_F(MergeNodeTest, keepsTheMatchedRowsBesideTheOnesItBound) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) MERGE (t:Tag {name: 'x'}) RETURN p.name, t.name",
                    {{"Remy", "x"}});
}

// The merge fans one row out over every Person, and count(*) tallies the rows it emitted
TEST_F(MergeNodeTest, countsTheRowsTheMergeEmitted) {
    expectWriteRows("MERGE (p:Person) RETURN count(*)", {{"8"}});
}

// A barrier publishes the rows the merge then runs over, one merge per published row
TEST_F(MergeNodeTest, writesANodePerRowABarrierPublished) {
    expectWriteRows("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 2 "
                    "MERGE (t:Tag {name: name}) "
                    "RETURN t.name",
                    {{"Adam"}, {"Cyrus"}});
}

// The grouping key is a column the merge produced, so each group is one merged node
TEST_F(MergeNodeTest, groupsTheRowsTheMergeEmitted) {
    expectWriteRows("MATCH (p:Person) MERGE (t:Tag {name: 'x'}) RETURN t.name, count(*)",
                    {{"x", "8"}});
}

// A property the pattern does not key on is null on the node it wrote
TEST_F(MergeNodeTest, readsNullForAPropertyTheWriteDidNotSet) {
    expectWriteRows("MERGE (n:Tag {name: 'x'}) RETURN n.age", {{"null"}});
}

// The same read on a node the pattern found reads the graph
TEST_F(MergeNodeTest, readsThePropertyOfTheNodeItFound) {
    expectWriteRows("MERGE (p:Person {name: 'Remy'}) RETURN p.age", {{"32"}});
}

// ON CREATE runs over the rows the merge wrote, ON MATCH over the rows it bound
TEST_F(MergeNodeTest, setsThePropertyOfTheRowsItWrote) {
    expectWriteRowCount("MERGE (n:Tag {name: 'x'}) ON CREATE SET n.age = 1", 0);

    expectRows("MATCH (t:Tag) RETURN t.name, t.age", {{"x", "1"}});
}

TEST_F(MergeNodeTest, leavesThePropertyOfTheRowsItBoundAlone) {
    expectWriteRowCount("MERGE (p:Person {name: 'Remy'}) ON CREATE SET p.age = 1", 0);

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"32"}});
}

TEST_F(MergeNodeTest, setsThePropertyOfTheRowsItBound) {
    expectWriteRowCount("MERGE (p:Person {name: 'Remy'}) ON MATCH SET p.age = 1", 0);

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"1"}});
}

TEST_F(MergeNodeTest, leavesThePropertyOfTheRowsItWroteAlone) {
    expectWriteRowCount("MERGE (n:Tag {name: 'x'}) ON MATCH SET n.age = 1", 0);

    expectRows("MATCH (t:Tag) RETURN t.name, t.age", {{"x", "null"}});
}

// A plain SET after a merge touches every row, whichever way the merge bound it
TEST_F(MergeNodeTest, setsThePropertyOfEveryRow) {
    expectWriteRowCount("MERGE (n:Tag {name: 'x'}) SET n.age = 2", 0);

    expectRows("MATCH (t:Tag) RETURN t.age", {{"2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
