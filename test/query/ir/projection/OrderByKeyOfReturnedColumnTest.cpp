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
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

const std::string_view aggregateReason =
    "ORDER BY with an aggregate may only order by expressions over the returned columns";

const std::string_view distinctReason =
    "ORDER BY with DISTINCT may only order by expressions over the returned columns";

}

class OrderByKeyOfReturnedColumnTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

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

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
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
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const Rows& actual = sink.rows();

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    void writeAttrs() {
        write("MATCH (n:Person {name: 'Remy'}) SET n.attrs = {a: 2}");
        write("MATCH (n:Person {name: 'Adam'}) SET n.attrs = {a: 1}");
    }

    void writeEvents() {
        write("CREATE (n:Event {name: 'a', at: datetime('2026-09-23T14:05:06Z')})");
        write("CREATE (n:Event {name: 'b', at: datetime('2019-01-02T03:04:05Z')})");
    }

    void writeTasks() {
        write("CREATE (n:Task {name: 'a', took: duration(2000000)})");
        write("CREATE (n:Task {name: 'b', took: duration(90061000000)})");
        write("CREATE (n:Task {name: 'c', took: duration(-1500000)})");
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAKeyOfAReturnedMap) {
    expectOrderedRows("MATCH (n:Person) RETURN {name: n.name} AS x, count(*) AS c ORDER BY x.name",
                      {{"{name: Adam}", "1"},
                       {"{name: Cyrus}", "1"},
                       {"{name: Doruk}", "1"},
                       {"{name: Luc}", "1"},
                       {"{name: Martina}", "1"},
                       {"{name: Maxime}", "1"},
                       {"{name: Remy}", "1"},
                       {"{name: Suhas}", "1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAKeyOfAReturnedMapDescending) {
    expectOrderedRows("MATCH (n:Person) RETURN {name: n.name} AS x, count(*) AS c ORDER BY x.name DESC LIMIT 3",
                      {{"{name: Suhas}", "1"},
                       {"{name: Remy}", "1"},
                       {"{name: Maxime}", "1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOfSeveralRowsOnAKeyOfAReturnedMap) {
    expectOrderedRows("MATCH (n:Person) RETURN {age: n.age} AS x, count(*) AS c ORDER BY x.age",
                      {{"{age: 32}", "2"},
                       {"{age: null}", "6"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAnExpressionOverAKeyOfAReturnedMap) {
    expectOrderedRows("MATCH (n:Person) RETURN {name: n.name} AS x, count(*) AS c ORDER BY size(x.name) DESC, x.name LIMIT 2",
                      {{"{name: Martina}", "1"},
                       {"{name: Maxime}", "1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAKeyOfAMapCarriedByWith) {
    expectOrderedRows("MATCH (n:Person) WITH {name: n.name} AS x, count(*) AS c ORDER BY x.name DESC LIMIT 2 "
                      "RETURN x.name, c",
                      {{"Suhas", "1"},
                       {"Remy", "1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersDistinctRowsOnAKeyOfAReturnedMap) {
    expectOrderedRows("MATCH (n:Person) RETURN DISTINCT {a: n.age} AS m ORDER BY m.a",
                      {{"{a: 32}"},
                       {"{a: null}"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersDistinctRowsOnAKeyOfAReturnedMapDescending) {
    expectOrderedRows("MATCH (n:Person) RETURN DISTINCT {name: n.name} AS m ORDER BY m.name DESC LIMIT 2",
                      {{"{name: Suhas}"},
                       {"{name: Remy}"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAKeyOfAReturnedMapProjection) {
    expectOrderedRows("MATCH (n:Person) RETURN n {.name} AS x, count(*) AS c ORDER BY x.name LIMIT 2",
                      {{"{name: Adam}", "1"},
                       {"{name: Cyrus}", "1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersDistinctRowsOnAKeyOfAReturnedMapProjection) {
    expectOrderedRows("MATCH (n:Person) RETURN DISTINCT n {.age} AS m ORDER BY m.age",
                      {{"{age: 32}"},
                       {"{age: null}"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAKeyOfAnAliasedStoredMap) {
    writeAttrs();

    expectOrderedRows("MATCH (n:Person) WHERE n.attrs IS NOT NULL RETURN n.attrs AS m, count(*) ORDER BY m.a",
                      {{"{a: 1}", "1"},
                       {"{a: 2}", "1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAKeyOfAReturnedStoredMap) {
    writeAttrs();

    expectOrderedRows("MATCH (n:Person) WHERE n.attrs IS NOT NULL RETURN n.attrs, count(*) ORDER BY n.attrs.a DESC",
                      {{"{a: 2}", "1"},
                       {"{a: 1}", "1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersDistinctRowsOnAKeyOfAReturnedStoredMap) {
    writeAttrs();

    expectOrderedRows("MATCH (n:Person) WHERE n.attrs IS NOT NULL RETURN DISTINCT n.attrs ORDER BY n.attrs.a",
                      {{"{a: 1}"},
                       {"{a: 2}"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAComponentOfAnAliasedDateTime) {
    writeEvents();

    expectOrderedRows("MATCH (n:Event) RETURN n.at AS d, count(*) ORDER BY d.year",
                      {{"2019-01-02T03:04:05Z", "1"},
                       {"2026-09-23T14:05:06Z", "1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersDistinctRowsOnAComponentOfAnAliasedDuration) {
    writeTasks();

    expectOrderedRows("MATCH (n:Task) RETURN DISTINCT n.took AS t ORDER BY t.seconds DESC",
                      {{"PT25H1M1S"},
                       {"PT2S"},
                       {"PT-1.5S"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersRowsOnAPropertyOfAComputedNode) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN startNode(e) AS x ORDER BY x.name LIMIT 3",
                      {{"1"},
                       {"1"},
                       {"1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAPropertyOfAComputedNode) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN startNode(e) AS x, count(*) AS c ORDER BY x.name LIMIT 3",
                      {{"1", "3"},
                       {"15", "2"},
                       {"17", "1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersDistinctRowsOnAPropertyOfAComputedNode) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT startNode(e) AS x ORDER BY x.name LIMIT 3",
                      {{"1"},
                       {"15"},
                       {"17"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnTheLabelsOfAComputedNode) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN startNode(e) AS x, count(*) AS c "
                      "ORDER BY size(labels(x)) DESC, x.name LIMIT 3",
                      {{"1", "3"},
                       {"0", "4"},
                       {"15", "2"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersRowsOnAPatternComprehensionOverAComputedNode) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN startNode(e) AS x ORDER BY size([(x)-->(y) | y]) DESC LIMIT 2",
                      {{"0"},
                       {"0"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAPatternComprehensionOverAComputedNode) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN startNode(e) AS x, count(*) AS c "
                      "ORDER BY size([(x)-->(y) | y]) DESC LIMIT 2",
                      {{"0", "4"},
                       {"1", "3"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnACountSubqueryOverAComputedNode) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN startNode(e) AS x, count(*) AS c "
                      "ORDER BY COUNT { (x)-->() } DESC LIMIT 2",
                      {{"0", "4"},
                       {"1", "3"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, aSiblingItemDoesNotSeeTheAlias) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN startNode(e) AS x, size([(x)-->(y) | y]) AS d ORDER BY x.name LIMIT 1",
                      {{"1", "18"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, readsAComputedNodeCarriedByWith) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) WITH DISTINCT startNode(e) AS x "
                      "RETURN x.name, size([(x)-->(y) | y]) ORDER BY x.name LIMIT 2",
                      {{"Adam", "3"},
                       {"Cyrus", "2"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAPropertyOfAnAliasedNode) {
    expectOrderedRows("MATCH (n:Person) RETURN n AS x, count(*) ORDER BY x.name LIMIT 2",
                      {{"1", "1"},
                       {"15", "1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAPatternComprehensionOverAnAliasedNode) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN n AS x, count(*) AS c ORDER BY size([(x)-->(y) | y]) DESC LIMIT 2",
                      {{"0", "4"},
                       {"1", "3"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAPatternComprehensionOverAVariable) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN n, count(*) AS c ORDER BY size([(n)-->(y) | y]) DESC LIMIT 2",
                      {{"0", "4"},
                       {"1", "3"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersGroupsOnAKeyOfAMapReturnedByName) {
    expectOrderedRows("MATCH (n:Person) WITH {name: n.name} AS m RETURN m, count(*) ORDER BY m.name LIMIT 2",
                      {{"{name: Adam}", "1"},
                       {"{name: Cyrus}", "1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersDistinctRowsOnAPatternComprehensionOverAVariable) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT n ORDER BY size([(n)-->(y) | y]) DESC LIMIT 2",
                      {{"0"},
                       {"1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersDistinctRowsOnACountSubqueryOverAVariable) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT n ORDER BY COUNT { (n)-->() } DESC LIMIT 2",
                      {{"0"},
                       {"1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersDistinctRowsOnAPatternComprehensionOverAComputedNode) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT startNode(e) AS x ORDER BY size([(x)-->(y) | y]) DESC LIMIT 2",
                      {{"0"},
                       {"1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, ordersDistinctRowsOnACountSubqueryOverAComputedNode) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT startNode(e) AS x ORDER BY COUNT { (x)-->() } DESC LIMIT 2",
                      {{"0"},
                       {"1"}});
}

TEST_F(OrderByKeyOfReturnedColumnTest, stillRejectsAPropertyOfAVariableBesideAReturnedMap) {
    expectError("MATCH (n:Person) RETURN {name: n.name} AS x, count(*) ORDER BY n.age",
                aggregateReason);
    expectError("MATCH (n:Person) RETURN DISTINCT {name: n.name} AS m ORDER BY n.age",
                distinctReason);
}

TEST_F(OrderByKeyOfReturnedColumnTest, stillRejectsAKeyOfAMapThatIsNotReturned) {
    expectError("MATCH (n:Person) WITH n, {name: n.name} AS x RETURN n.age, count(*) ORDER BY x.name",
                aggregateReason);
    expectError("MATCH (n:Person) WITH n, {name: n.name} AS x RETURN DISTINCT n.age ORDER BY x.name",
                distinctReason);
}

TEST_F(OrderByKeyOfReturnedColumnTest, stillRejectsAKeyOfAStoredMapThatIsNotReturned) {
    writeAttrs();

    expectError("MATCH (n:Person) RETURN n.name, count(*) ORDER BY n.attrs.a",
                aggregateReason);
    expectError("MATCH (n:Person) RETURN DISTINCT n.name ORDER BY n.attrs.a",
                distinctReason);
}

TEST_F(OrderByKeyOfReturnedColumnTest, stillRejectsAPropertyOfAVariableBesideAComputedNode) {
    expectError("MATCH (n:Person)-[e]->(m) RETURN startNode(e) AS x, count(*) ORDER BY m.name",
                aggregateReason);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
