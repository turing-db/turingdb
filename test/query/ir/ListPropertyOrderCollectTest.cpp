#include <gtest/gtest.h>

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
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A stored list used as more than a value read back: as the key an ORDER BY sorts on, and
// as the value a collect gathers. Both already work for a list a query built - a collected
// one rides a plain list column - and a property's rides a nullable one, which is the
// column these exercise. Rows are checked in emit order, so the sort itself is the subject.
class ListPropertyOrderCollectTest : public TuringTest {
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

    void write(std::string_view query) {
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

        submit(changeID);
    }

    void expectOrderedRows(std::string_view query, const Rows& expected) {
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

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void tagThreeNodes() {
        write("MATCH (n {name: 'Remy'}) SET n.tags = [2, 1]");
        write("MATCH (n {name: 'Adam'}) SET n.tags = [1, 9]");
        write("MATCH (n {name: 'Maxime'}) SET n.tags = [1, 2]");
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(ListPropertyOrderCollectTest, ordersTheRowsByTheStoredList) {
    tagThreeNodes();
    expectOrderedRows("MATCH (n) WHERE n.tags IS NOT NULL RETURN n.name ORDER BY n.tags",
                      {{"Maxime"}, {"Adam"}, {"Remy"}});
}

TEST_F(ListPropertyOrderCollectTest, ordersTheRowsByTheStoredListDescending) {
    tagThreeNodes();
    expectOrderedRows("MATCH (n) WHERE n.tags IS NOT NULL RETURN n.name ORDER BY n.tags DESC",
                      {{"Remy"}, {"Adam"}, {"Maxime"}});
}

// A prefix sorts before the longer list it opens, as ListElementOrder has it.
TEST_F(ListPropertyOrderCollectTest, ordersAPrefixBeforeTheLongerList) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [1, 2]");
    write("MATCH (n {name: 'Adam'}) SET n.tags = [1]");
    expectOrderedRows("MATCH (n) WHERE n.tags IS NOT NULL RETURN n.name ORDER BY n.tags",
                      {{"Adam"}, {"Remy"}});
}

TEST_F(ListPropertyOrderCollectTest, ordersTheEmptyListFirst) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [1]");
    write("MATCH (n {name: 'Adam'}) SET n.tags = []");
    expectOrderedRows("MATCH (n) WHERE n.tags IS NOT NULL RETURN n.name ORDER BY n.tags",
                      {{"Adam"}, {"Remy"}});
}

TEST_F(ListPropertyOrderCollectTest, ordersNestedListsElementWise) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [[1, 2], [3]]");
    write("MATCH (n {name: 'Adam'}) SET n.tags = [[1, 1], [4]]");
    expectOrderedRows("MATCH (n) WHERE n.tags IS NOT NULL RETURN n.name ORDER BY n.tags",
                      {{"Adam"}, {"Remy"}});
}

// A node holding no list sorts after every list, as a null value does.
TEST_F(ListPropertyOrderCollectTest, ordersTheNodesHoldingNoListLast) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [1]");
    expectOrderedRows("MATCH (n {name: 'Remy'}) RETURN n.name ORDER BY n.tags", {{"Remy"}});
    expectOrderedRows("MATCH (n) WHERE n.name = 'Remy' OR n.name = 'Adam' "
                      "RETURN n.name ORDER BY n.tags",
                      {{"Remy"}, {"Adam"}});
}

TEST_F(ListPropertyOrderCollectTest, ordersTheStoredListsBesideAnAggregate) {
    tagThreeNodes();
    expectOrderedRows("MATCH (n) WHERE n.tags IS NOT NULL RETURN n.tags, count(n) ORDER BY n.tags",
                      {{"[1, 2]", "1"}, {"[1, 9]", "1"}, {"[2, 1]", "1"}});
}

TEST_F(ListPropertyOrderCollectTest, collectsTheStoredLists) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [1, 2]");
    write("MATCH (n {name: 'Adam'}) SET n.tags = [3]");
    expectOrderedRows("MATCH (n) WHERE n.tags IS NOT NULL RETURN collect(n.tags)",
                      {{"[[1, 2], [3]]"}});
}

// collect drops nulls, so a node holding no list contributes nothing.
TEST_F(ListPropertyOrderCollectTest, collectDropsTheNodesHoldingNoList) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [1, 2]");
    expectOrderedRows("MATCH (n) RETURN collect(n.tags)", {{"[[1, 2]]"}});
}

TEST_F(ListPropertyOrderCollectTest, collectsAnEmptyListAsAnElement) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = []");
    expectOrderedRows("MATCH (n) RETURN collect(n.tags)", {{"[[]]"}});
}

TEST_F(ListPropertyOrderCollectTest, collectsNestedStoredLists) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [[1, 2], [3]]");
    expectOrderedRows("MATCH (n) RETURN collect(n.tags)", {{"[[[1, 2], [3]]]"}});
}

TEST_F(ListPropertyOrderCollectTest, collectsTheStoredListsPerGroup) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [1]");
    write("MATCH (n {name: 'Adam'}) SET n.tags = [2]");
    expectOrderedRows("MATCH (n) WHERE n.tags IS NOT NULL RETURN n.name, collect(n.tags) "
                      "ORDER BY n.name",
                      {{"Adam", "[[2]]"}, {"Remy", "[[1]]"}});
}

// Two nodes holding equal lists collect once under DISTINCT: the key is the list's
// contents, not the buffer its view points into.
TEST_F(ListPropertyOrderCollectTest, collectsDistinctStoredLists) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [1, 2]");
    write("MATCH (n {name: 'Adam'}) SET n.tags = [1, 2]");
    write("MATCH (n {name: 'Maxime'}) SET n.tags = [3]");
    expectOrderedRows("MATCH (n) WHERE n.tags IS NOT NULL RETURN collect(DISTINCT n.tags)",
                      {{"[[1, 2], [3]]"}});
}

TEST_F(ListPropertyOrderCollectTest, collectsTheStoredListsOfAnEdge) {
    write("MATCH (a {name: 'Remy'})-[e:KNOWS_WELL]->(b) SET e.tags = [7]");
    expectOrderedRows("MATCH (a)-[e:KNOWS_WELL]->(b) WHERE e.tags IS NOT NULL "
                      "RETURN collect(e.tags)",
                      {{"[[7]]"}});
}

// The collected list is a list like any other, so it unwinds back to the stored ones.
TEST_F(ListPropertyOrderCollectTest, unwindsTheCollectedStoredLists) {
    write("MATCH (n {name: 'Remy'}) SET n.tags = [1, 2]");
    write("MATCH (n {name: 'Adam'}) SET n.tags = [3]");
    expectOrderedRows("MATCH (n) WHERE n.tags IS NOT NULL WITH collect(n.tags) AS gathered "
                      "UNWIND gathered AS one RETURN one",
                      {{"[1, 2]"}, {"[3]"}});
}
