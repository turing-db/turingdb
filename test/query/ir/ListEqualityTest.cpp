#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class ListEqualityTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectRows(std::string_view query, std::vector<StringRowSink::Row> expected) {
        StringRowSink sink;
        QueryStatus status;

        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ListEqualityTest, elementsPairwiseInOrder) {
    expectRows("RETURN [1, 2, 3] = [1, 2, 3]", {{"true"}});
    expectRows("RETURN ['a', 'b'] = ['a', 'b']", {{"true"}});
    expectRows("RETURN [] = []", {{"true"}});

    // Order is part of a list's value, where it is not part of a set's
    expectRows("RETURN [1, 2] = [2, 1]", {{"false"}});
    expectRows("RETURN [1, 2] = [1, 3]", {{"false"}});
}

// A shorter list differs from a longer one whatever they share, so the lengths settle it
// before any element is read
TEST_F(ListEqualityTest, listsOfDifferentLengthDiffer) {
    expectRows("RETURN [1, 2] = [1, 2, 3]", {{"false"}});
    expectRows("RETURN [] = [1]", {{"false"}});
}

TEST_F(ListEqualityTest, mixedAndNestedElements) {
    expectRows("RETURN [1, 'a', true] = [1, 'a', true]", {{"true"}});
    expectRows("RETURN [1, 'a', true] = [1, 'a', false]", {{"false"}});

    expectRows("RETURN [[1, 2], [3]] = [[1, 2], [3]]", {{"true"}});
    expectRows("RETURN [[1, 2], [3]] = [[1, 2], [4]]", {{"false"}});
}

TEST_F(ListEqualityTest, inequality) {
    expectRows("RETURN [1, 2] <> [1, 3]", {{"true"}});
    expectRows("RETURN [1, 2] <> [1, 2]", {{"false"}});
    expectRows("RETURN ['a'] <> ['a', 'b']", {{"true"}});
}

// A null element is an unknown value rather than one that differs, so it leaves the answer
// unknown - unless another position already settles it
TEST_F(ListEqualityTest, nullElementsAreUnknown) {
    expectRows("RETURN [1, null] = [1, null]", {{"null"}});
    expectRows("RETURN [1] = [null]", {{"null"}});
    expectRows("RETURN [1, 2] = [1, null]", {{"null"}});

    // The first position differs, so nothing the null could stand for changes the answer
    expectRows("RETURN [1, null] = [2, null]", {{"false"}});

    // Different lengths differ before any element is read, null or not
    expectRows("RETURN [1, null] = [1, null, 3]", {{"false"}});

    // Negating an unknown answer leaves it unknown
    expectRows("RETURN [1, null] <> [1, null]", {{"null"}});
}

// The list a collect gathers compares like a literal one: the UNWIND fixes the order, so
// the gathered list is the one the literal spells
TEST_F(ListEqualityTest, collectedListEquality) {
    expectRows("UNWIND [1, 2, 3] AS x WITH collect(x) AS xs RETURN xs = [1, 2, 3]", {{"true"}});
    expectRows("UNWIND [1, 2, 3] AS x WITH collect(x) AS xs RETURN xs = [3, 2, 1]", {{"false"}});
}

TEST_F(ListEqualityTest, filtersOnAListEquality) {
    expectRows("UNWIND [1, 2] AS x WITH collect(x) AS xs WHERE xs = [1, 2] RETURN xs",
               {{"1, 2"}});

    expectRows("UNWIND [1, 2] AS x WITH collect(x) AS xs WHERE xs = [2, 1] RETURN xs", {});

    // An unknown answer keeps no row, as a null predicate does anywhere else
    expectRows("UNWIND [1, 2] AS x WITH collect(x) AS xs WHERE xs = [1, null] RETURN xs", {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
