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

// A projection reading what a CREATE bound: the rows a CREATE ... RETURN emits, over the
// nodes and edges the pattern wrote rather than over the ones a MATCH found.
class CreateReturnTest : public TuringTest {
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

    // For the entity columns, whose IDs the graph hands out: the count of rows is what the
    // projection is asked about, not the values
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

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(CreateReturnTest, returnsAPropertyOfTheNodeItCreated) {
    expectWriteRows("CREATE (n:Tag {name: 'x'}) RETURN n.name", {{"x"}});
}

TEST_F(CreateReturnTest, returnsTheNodeItCreated) {
    expectWriteRows("CREATE (n:Tag) RETURN n", {{"18"}});
}

TEST_F(CreateReturnTest, multiCreate) {
    expectWriteRows("CREATE (n:Tag) CREATE (m:Tag) RETURN n, m", {{"18", "19"}});
    expectWriteRows("CREATE (n:Tag) CREATE (m:Tag) RETURN n, m", {{"20", "21"}});
    expectWriteRows("CREATE (s:Src)-[e:E]->(t:Tgt) RETURN s, e, t", {{"22", "18", "23"}});
    expectWriteRows("CREATE (a:A) CREATE (a)-[e:E]->(b:B) RETURN a, e, b", {{"24", "19", "25"}});
    expectWriteRows("MATCH (n:Src), (m:Tgt) CREATE (n)<-[e:New]-(m) RETURN e", {{"20"}});

    expectWriteRows("commit", {});
    expectWriteRows("MATCH (a:A)-[e:E]->(b:B) RETURN *", {{"24", "19", "25"}});
}

// The CREATE wrote no age, and a property it did not write is null rather than whatever
// the graph holds for the ID the provisional one collides with
TEST_F(CreateReturnTest, returnsNullForAPropertyTheCreateDidNotWrite) {
    expectWriteRows("CREATE (n:Tag {name: 'x'}) RETURN n.age", {{"null"}});
}

// One Tag per Person, each carrying that Person's name
TEST_F(CreateReturnTest, returnsANodePerMatchedRow) {
    expectWriteRows("MATCH (p:Person) CREATE (t:Tag {name: p.name}) RETURN t.name",
                    {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"},
                     {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}});
}

// The rows a barrier published drive the write, and the projection reads the nodes it wrote
TEST_F(CreateReturnTest, returnsANodeCreatedOverTheRowsABarrierPublished) {
    expectWriteRows("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 2 "
                    "CREATE (t:Tag {name: name}) "
                    "RETURN t.name",
                    {{"Adam"}, {"Cyrus"}});
}

TEST_F(CreateReturnTest, returnsBothEndsOfThePatternItCreated) {
    expectWriteRows("CREATE (a:Tag {name: 'a'})-[:LINK]->(b:Tag {name: 'b'}) "
                    "RETURN a.name, b.name",
                    {{"a", "b"}});
}

TEST_F(CreateReturnTest, returnsAPropertyOfTheEdgeItCreated) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) "
                    "CREATE (a)-[e:KNOWS_WELL {name: 'Remy -> Luc'}]->(b) "
                    "RETURN e.name",
                    {{"Remy -> Luc"}});
}

// The node the projection returned is the node the change wrote
TEST_F(CreateReturnTest, returnsTheNodeTheChangeKept) {
    expectWriteRows("CREATE (n:Tag {name: 'kept'}) RETURN n.name", {{"kept"}});

    expectRows("MATCH (t:Tag) RETURN t.name", {{"kept"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv,
                                        [] { testing::GTEST_FLAG(repeat) = 5; });
}
