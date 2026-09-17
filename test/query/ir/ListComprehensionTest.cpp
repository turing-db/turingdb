#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "iterators/ChunkConfig.h"
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

// `[x IN xs WHERE p(x) | f(x)]` builds one list per row out of the elements of that row's
// list, whatever the elements are and whatever the body reads beside them.
class ListComprehensionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
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

    // Writes in a change of its own and submits it, so the read that follows sees a
    // committed list rather than the column the writing query happened to build - the
    // shape ListPropertyTest uses.
    void write(std::string_view query) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const auto res = system.newChange(_graphName);
            ASSERT_TRUE(res);

            changeID = res.value()->id();
        }

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

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "CHANGE SUBMIT failed";
    }

    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(ListComprehensionTest, projectsEveryElement) {
    expectRows("RETURN [x IN [1,2,3] | x * 2]", {{"[2, 4, 6]"}});
}

TEST_F(ListComprehensionTest, filtersWithoutProjecting) {
    expectRows("RETURN [x IN [1,2,3,4] WHERE x > 2]", {{"[3, 4]"}});
}

TEST_F(ListComprehensionTest, filtersAndProjects) {
    expectRows("RETURN [x IN [1,2,3,4] WHERE x > 1 | x * 10]", {{"[20, 30, 40]"}});
}

TEST_F(ListComprehensionTest, keepsNoElement) {
    expectRows("RETURN [x IN [1,2,3] WHERE x > 10]", {{"[]"}});
}

TEST_F(ListComprehensionTest, iteratesAnEmptyList) {
    expectRows("RETURN [x IN [] | x]", {{"[]"}});
}

TEST_F(ListComprehensionTest, iteratesNull) {
    expectRows("RETURN [x IN null | x]", {{"null"}});
}

TEST_F(ListComprehensionTest, iteratesStrings) {
    expectRows("RETURN [x IN ['a','b','c'] WHERE x <> 'b']", {{"[a, c]"}});
}

TEST_F(ListComprehensionTest, iteratesAListBoundByWith) {
    expectRows("WITH [1,2,3] AS l RETURN [x IN l | x + 1]", {{"[2, 3, 4]"}});
}

TEST_F(ListComprehensionTest, readsAnOuterPropertyInTheProjection) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [x IN [1,2] | x + n.age]", {{"[33, 34]"}});
}

TEST_F(ListComprehensionTest, readsAnOuterPropertyInThePredicate) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [x IN [30,32,34] WHERE x = n.age]", {{"[32]"}});
}

TEST_F(ListComprehensionTest, buildsTheSameListOnEveryRow) {
    expectRows("MATCH (n:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WHERE n.name = 'Adam' "
               "RETURN i.name, [x IN [1,2] | x]",
               {{"Bio", "[1, 2]"},
                {"Cooking", "[1, 2]"}});
}

TEST_F(ListComprehensionTest, projectsOverTheRowItIsReadOn) {
    expectRows("MATCH (n:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WHERE n.name = 'Adam' "
               "RETURN i.name, [x IN [1,2] | i.name]",
               {{"Bio", "[Bio, Bio]"},
                {"Cooking", "[Cooking, Cooking]"}});
}

TEST_F(ListComprehensionTest, filtersOverTheRowItIsReadOn) {
    expectRows("MATCH (n:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WHERE n.name = 'Adam' "
               "RETURN i.name, [x IN ['Bio','Padel'] WHERE x = i.name]",
               {{"Bio", "[Bio]"},
                {"Cooking", "[]"}});
}

TEST_F(ListComprehensionTest, filtersRowsByTheSizeOfItsResult) {
    expectRows("MATCH (n:Person) WHERE size([x IN [1,2,3] WHERE x > n.age - 31]) = 2 "
               "RETURN n.name",
               {{"Remy"}, {"Adam"}});
}

TEST_F(ListComprehensionTest, iteratesAnAggregateOfItsOwnProjection) {
    expectRows("MATCH (n:Person) RETURN [x IN collect(n.name) WHERE x = 'Remy']", {{"[Remy]"}});
}

TEST_F(ListComprehensionTest, iteratesACollectedList) {
    expectRows("MATCH (n:Person) WITH collect(n.name) AS names "
               "RETURN [x IN names WHERE x = 'Remy']",
               {{"[Remy]"}});
}

TEST_F(ListComprehensionTest, readsAPropertyOfACollectedNode) {
    expectRows("MATCH (n:Person) WITH collect(n) AS people "
               "RETURN [x IN people WHERE x.name = 'Remy' | x.age]",
               {{"[32]"}});
}

TEST_F(ListComprehensionTest, nestsOneComprehensionInAnother) {
    expectRows("RETURN [x IN [[1,2],[3]] | [y IN x | y * 2]]", {{"[[2, 4], [6]]"}});
}

TEST_F(ListComprehensionTest, readsTheOuterElementFromANestedComprehension) {
    expectRows("RETURN [x IN [1,2] | [y IN [10,20] | x + y]]", {{"[[11, 21], [12, 22]]"}});
}

TEST_F(ListComprehensionTest, sizesItsResult) {
    expectRows("RETURN size([x IN [1,2,3] WHERE x > 1])", {{"2"}});
}

TEST_F(ListComprehensionTest, unwindsItsResult) {
    expectRows("UNWIND [x IN [1,2,3] WHERE x > 1 | x * 2] AS y RETURN y", {{"4"}, {"6"}});
}

TEST_F(ListComprehensionTest, keepsTheNullElementsOfItsSource) {
    expectRows("RETURN [x IN [1,null,3] | x]", {{"[1, null, 3]"}});
}

TEST_F(ListComprehensionTest, dropsTheElementItsPredicateReadsNullFor) {
    expectRows("RETURN [x IN [1,null,3] WHERE x > 0]", {{"[1, 3]"}});
}

TEST_F(ListComprehensionTest, iteratesAStoredListProperty) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2, 3]})");
    expectRows("MATCH (n:Tagged) RETURN [x IN n.tags WHERE x > 1]", {{"[2, 3]"}});
}

TEST_F(ListComprehensionTest, iteratesARowThatHasNoList) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2]})");
    write("CREATE (n:Tagged {name: 'b'})");
    expectRows("MATCH (n:Tagged) RETURN n.name, [x IN n.tags | x]",
               {{"a", "[1, 2]"},
                {"b", "null"}});
}

// One row's elements outgrow a chunk and the next row's start mid-chunk, so a list is
// complete only if the elements of the chunks it spans are gathered under its own row.
TEST_F(ListComprehensionTest, buildsEachListAcrossTheChunksItsElementsSpan) {
    const std::vector<size_t> chunkSizes {1, 2, 3, 5, ChunkConfig::CHUNK_SIZE};

    for (const size_t chunkSize : chunkSizes) {
        _interpreter->setChunkSize(chunkSize);

        expectRows("MATCH (n:Person)-[:INTERESTED_IN]->(i:Interest) "
                   "WHERE n.name = 'Adam' "
                   "RETURN i.name, [x IN [1,2,3,4,5,6,7] WHERE x > 2 | x * 2]",
                   {{"Bio", "[6, 8, 10, 12, 14]"},
                    {"Cooking", "[6, 8, 10, 12, 14]"}});
    }
}

// The WHERE cuts the elements before the projection runs over them, so an element it
// drops is one the projection never sees - here, never divides by.
TEST_F(ListComprehensionTest, projectsOnlyTheElementsItKeeps) {
    expectRows("RETURN [x IN [0,1,2] WHERE x <> 0 | 10 / x]", {{"[10, 5]"}});
}

TEST_F(ListComprehensionTest, carriesItsListPastAWithBarrier) {
    expectRows("WITH [x IN [1,2,3] WHERE x > 1] AS l RETURN l, size(l)", {{"[2, 3]", "2"}});
}

TEST_F(ListComprehensionTest, ordersAndCutsTheRowsItBuiltListsFor) {
    expectRows("MATCH (n:Person) WITH n, [x IN [1,2] | n.name] AS l "
               "RETURN n.name, l ORDER BY n.name LIMIT 2",
               {{"Adam", "[Adam, Adam]"},
                {"Cyrus", "[Cyrus, Cyrus]"}});
}

TEST_F(ListComprehensionTest, namesTheVariableItAlreadyDeclared) {
    expectError("MATCH (x:Person) RETURN [x IN [1,2] | x]", "already declared");
}

TEST_F(ListComprehensionTest, iteratesSomethingThatIsNoList) {
    expectError("RETURN [x IN 3 | x]", "iterates a list");
}

TEST_F(ListComprehensionTest, dropsItsVariableAfterTheBody) {
    expectError("RETURN [y IN [1,2] | y], y", "not found");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
