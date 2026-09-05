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
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// A list of 500 integers, comma-separated - the spelling both the query literal and the
// rendered cell use. A ListBuffer chunk holds 4096 bytes and an integer element costs a
// one-byte tag beside its eight bytes, so this many no longer fit one chunk and the
// property container has to reserve a fresh one as it copies the list in.
std::string longListElements() {
    constexpr size_t elementCount = 500;

    std::string elements;
    for (size_t element = 1; element <= elementCount; element++) {
        if (element > 1) {
            elements += ", ";
        }

        elements += std::to_string(element);
    }

    return elements;
}

}

// Lists stored as properties: what a CREATE or a SET writes into a datapart's
// ListContainer, and what a later MATCH reads back out of it. Each write runs in its own
// change and is submitted, so the read that follows sees a committed value rather than
// the column the writing query happened to build.
class ListPropertyTest : public TuringTest {
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

TEST_F(ListPropertyTest, storesAnIntegerList) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2, 3]})");
    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"[1, 2, 3]"}});
}

TEST_F(ListPropertyTest, storesAStringList) {
    write("CREATE (n:Tagged {name: 'a', tags: ['x', 'yy', 'zzz']})");
    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"[x, yy, zzz]"}});
}

TEST_F(ListPropertyTest, storesAHeterogeneousList) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 'two', true, 3.5]})");
    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"[1, two, true, 3.500000]"}});
}

TEST_F(ListPropertyTest, storesAListHoldingNull) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, null, 3]})");
    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"[1, null, 3]"}});
}

TEST_F(ListPropertyTest, storesANestedList) {
    write("CREATE (n:Tagged {name: 'a', tags: [[1, 2], [3]]})");
    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"[[1, 2], [3]]"}});
}

TEST_F(ListPropertyTest, storesAnEmptyList) {
    write("CREATE (n:Tagged {name: 'a', tags: []})");
    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"[]"}});
}

TEST_F(ListPropertyTest, storesAListLongerThanOneBufferChunk) {
    const std::string elements = longListElements();

    write("CREATE (n:Tagged {name: 'a', tags: [" + elements + "]})");
    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"[" + elements + "]"}});
}

TEST_F(ListPropertyTest, storesOneListPerNode) {
    write("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    write("CREATE (b:Tagged {name: 'b', tags: ['x']})");

    expectRows("MATCH (n:Tagged) RETURN n.name, n.tags",
               {{"a", "[1, 2]"}, {"b", "[x]"}});
}

TEST_F(ListPropertyTest, readsNullWhereTheNodeHasNoList) {
    write("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    write("CREATE (b:Tagged {name: 'b'})");

    expectRows("MATCH (n:Tagged) RETURN n.name, n.tags",
               {{"a", "[1, 2]"}, {"b", "null"}});
}

TEST_F(ListPropertyTest, setsAListOnAMatchedNode) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.tags = [7, 8]");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.tags", {{"[7, 8]"}});
}

TEST_F(ListPropertyTest, replacesAStoredList) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2]})");
    write("MATCH (n:Tagged) SET n.tags = [3]");

    expectRows("MATCH (n:Tagged) RETURN n.tags", {{"[3]"}});
}

TEST_F(ListPropertyTest, storesAListOnAnEdge) {
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
          "CREATE (a)-[e:TAGGED {tags: [1, 2]}]->(b)");

    expectRows("MATCH (:Person)-[e:TAGGED]->(:Person) RETURN e.tags", {{"[1, 2]"}});
}

TEST_F(ListPropertyTest, countsTheDistinctListsStored) {
    write("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");
    write("CREATE (b:Tagged {name: 'b', tags: [1, 2]})");
    write("CREATE (c:Tagged {name: 'c', tags: [3]})");

    // Two rows key alike because their elements are equal, not because they view the same
    // buffer: each list was copied into its own datapart.
    expectRows("MATCH (n:Tagged) RETURN count(DISTINCT n.tags)", {{"2"}});
}

TEST_F(ListPropertyTest, copiesAStoredListOntoAnotherNode) {
    write("CREATE (a:Tagged {name: 'a', tags: [1, 2]})");

    // The value column here is the nullable one a property fetch produces, so the list is
    // read out of one datapart and written into the next.
    write("MATCH (a:Tagged {name: 'a'}) CREATE (b:Copied {name: 'b', tags: a.tags})");

    expectRows("MATCH (n:Copied) RETURN n.tags", {{"[1, 2]"}});
}

TEST_F(ListPropertyTest, unwindsAStoredList) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2, 3]})");
    expectRows("MATCH (n:Tagged) UNWIND n.tags AS tag RETURN tag",
               {{"1"}, {"2"}, {"3"}});
}
