#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <spdlog/fmt/bundled/format.h>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Row = std::vector<std::string>;
using Rows = std::vector<Row>;
using ColumnNames = std::vector<std::string>;

// An embedding reads as the parenthesised run of floats the query spells it with, so an
// embedding element never reads the same as the nested list of the same numbers.
std::string renderEmbedding(const types::Embedding::Primitive floats) {
    std::string rendered = "(";
    for (size_t index = 0; index < floats.size(); index++) {
        if (index > 0) {
            rendered += ", ";
        }

        rendered += fmt::format("{}", floats[index]);
    }

    return rendered + ")";
}

std::string renderList(ListView list);

std::string renderListElement(const ListElementView element) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return fmt::format("{}", element.getAs<int64_t>());
        break;

        case ListBufferTypeTag::Double:
            return fmt::format("{}", element.getAs<double>());
        break;

        case ListBufferTypeTag::Bool:
            return element.getAs<bool>() ? "true" : "false";
        break;

        case ListBufferTypeTag::String:
            return "'" + std::string(element.getAs<std::string_view>()) + "'";
        break;

        case ListBufferTypeTag::Embedding:
            return renderEmbedding(element.getAs<types::Embedding::Primitive>());
        break;

        case ListBufferTypeTag::ListView:
            return renderList(element.getAs<ListView>());
        break;

        case ListBufferTypeTag::Null:
            return "null";
        break;

        default:
            throw TuringException("ReturnHeterogeneousListTest: unsupported list element tag");
        break;
    }
}

std::string renderList(const ListView list) {
    std::string rendered = "[";
    for (size_t index = 0; const ListElementView& element : list) {
        if (index > 0) {
            rendered += ", ";
        }

        rendered += renderListElement(element);
        index++;
    }

    return rendered + "]";
}

// Render one output cell: the list a literal holds in every row, the embedding a
// standalone one holds, a node ID or a property value beside them.
std::string renderCell(const Column* column, size_t row) {
    if (const auto* lists = dynamic_cast<const ColumnConst<ListView>*>(column)) {
        return renderList((*lists)[row]);
    }

    if (const auto* listRows = dynamic_cast<const ColumnVector<ListView>*>(column)) {
        return renderList((*listRows)[row]);
    }

    if (const auto* embeddings = dynamic_cast<const ColumnConst<types::Embedding::Primitive>*>(column)) {
        return renderEmbedding((*embeddings)[row]);
    }

    if (const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(column)) {
        return std::to_string((*counts)[row]);
    }

    if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
        return std::to_string((*nodeIDs)[row].getValue());
    }

    if (const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(column)) {
        const std::optional<std::string_view>& name = (*names)[row];
        return name ? std::string(*name) : "null";
    }

    throw TuringException("ReturnHeterogeneousListTest: unsupported output column type");
}

class CollectingListSink : public NLOutputSink {
public:
    void setColumnNames(std::span<const std::string_view> names) override {
        _names.assign(names.begin(), names.end());
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            Row& row = _rows.emplace_back();
            for (const Column* column : chunks) {
                row.push_back(renderCell(column, rowIndex));
            }
        }
    }

    const Rows& getRows() const { return _rows; }
    const ColumnNames& getNames() const { return _names; }

private:
    Rows _rows;
    ColumnNames _names;
};

}

// The query test suite's return-hetero-list case on the v3 engine:
// RETURN [true, "i love turingdb", 20.3, (1,2,3,4)] AS list
//
// Its fourth element is an embedding literal, a value no other list element is: the
// elements of the list are written into the list buffer, and this one carries a whole run
// of floats rather than a single scalar. The list shares no element type, so it is
// type-erased and every element keeps its own tag - the embedding's among them.
//
// The embedding literal is read as a column of its own here too, so that the run it holds
// is pinned wherever the query puts it.
class ReturnHeterogeneousListTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, CollectingListSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        CollectingListSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    void expectNamedRows(std::string_view query, const ColumnNames& names, const Rows& expected) {
        CollectingListSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getNames(), names) << "query: " << query;
        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// return-hetero-list: the reported query, aliased column name and all
TEST_F(ReturnHeterogeneousListTest, returnsTheReportedList) {
    const Rows expected = {{"[true, 'i love turingdb', 20.3, (1, 2, 3, 4)]"}};
    expectNamedRows("RETURN [true, \"i love turingdb\", 20.3, (1,2,3,4)] AS list", {"list"}, expected);
}

TEST_F(ReturnHeterogeneousListTest, returnsAnEmbeddingAsTheOnlyElement) {
    const Rows expected = {{"[(1, 2, 3, 4)]"}};
    expectRows("RETURN [(1,2,3,4)]", expected);
}

// Two embeddings of different lengths: each element carries its own run, so the second is
// not read against the first's length.
TEST_F(ReturnHeterogeneousListTest, returnsTwoEmbeddingsOfDifferentLengths) {
    const Rows expected = {{"[(1, 2), (3, 4, 5)]"}};
    expectRows("RETURN [(1,2), (3,4,5)]", expected);
}

TEST_F(ReturnHeterogeneousListTest, returnsFractionalEmbeddingElements) {
    const Rows expected = {{"[(0.5, -1.25), 20.3]"}};
    expectRows("RETURN [(0.5, -1.25), 20.3]", expected);
}

// The same literal as a column of its own and as an element of the list beside it: the two
// read the same run of floats.
TEST_F(ReturnHeterogeneousListTest, returnsTheEmbeddingBesideTheListHoldingIt) {
    const Rows expected = {{"(1, 2, 3)", "[(1, 2, 3)]"}};
    expectRows("RETURN (1,2,3), [(1,2,3)]", expected);
}

// An embedding inside a nested list is written while the parent is still being filled, so
// the child's run has to stay put as the parent's remaining elements land after it.
TEST_F(ReturnHeterogeneousListTest, keepsAnEmbeddingInsideANestedList) {
    const Rows expected = {{"[[(1, 2)], 3]"}};
    expectRows("RETURN [[(1,2)], 3]", expected);
}

TEST_F(ReturnHeterogeneousListTest, returnsAnEmbeddingBesideANull) {
    const Rows expected = {{"[null, (1, 2)]"}};
    expectRows("RETURN [null, (1,2)]", expected);
}

// The list is a value, not a source: it stands for the one matched row rather than
// spreading over the elements it holds.
TEST_F(ReturnHeterogeneousListTest, returnsTheListBesideAMatchedRow) {
    const Rows expected = {{"[true, (1, 2)]", "Remy"}};
    expectRows("MATCH (n) WHERE n.name = 'Remy' RETURN [true, (1,2)], n.name", expected);
}

// One row per matched node, each holding the same list: SimpleGraph holds 18 nodes.
TEST_F(ReturnHeterogeneousListTest, repeatsTheListForEveryMatchedNode) {
    constexpr uint64_t nodeCount = 18;

    Rows expected;
    for (uint64_t nodeID = 0; nodeID < nodeCount; nodeID++) {
        expected.push_back({std::to_string(nodeID), "[20.3, (1, 2, 3, 4)]"});
    }

    expectRows("MATCH (n) RETURN n, [20.3, (1,2,3,4)]", expected);
}

// The embedding as a grouping key beside the tally of its rows: the count reads the
// column, so the embedding is laid out per row rather than only projected.
TEST_F(ReturnHeterogeneousListTest, countsTheRowsOfAnEmbeddingKey) {
    const Rows expected = {{"(1, 2, 3)", "1"}};
    expectNamedRows("RETURN (1,2,3) AS l, count(l) AS n", {"l", "n"}, expected);
}

// The same query over an embedding whose floats are all equal. MLIR uniques an all-equal
// dense buffer down to one element, so the run has to be read back through the length its
// type carries rather than through the bytes the attribute stored.
TEST_F(ReturnHeterogeneousListTest, countsTheRowsOfARepeatedEmbeddingKey) {
    const Rows expected = {{"(1, 1, 1)", "1"}};
    expectNamedRows("RETURN (1,1,1) AS l, count(l) AS n", {"l", "n"}, expected);
}

// The embedding constant stands for every matched row, so its tally is the relation's 18
// and not the one row the cell is on its own.
TEST_F(ReturnHeterogeneousListTest, countsTheMatchedRowsAnEmbeddingStandsFor) {
    const Rows expected = {{"18"}};
    expectNamedRows("MATCH (n) RETURN count((1,2,3)) AS c", {"c"}, expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
