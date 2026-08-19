#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Row = std::vector<std::string>;
using Rows = std::vector<Row>;

// Render one cell of a nullable value column of primitive T, or nothing when the column
// has another element type.
template <typename T>
std::optional<std::string> renderOptCell(const Column* column, size_t row) {
    const auto* values = dynamic_cast<const ColumnOptVector<T>*>(column);
    if (!values) {
        return std::nullopt;
    }

    const std::optional<T>& value = (*values)[row];
    if (!value) {
        return "null";
    }

    if constexpr (std::is_same_v<T, CustomBool>) {
        return static_cast<bool>(*value) ? "true" : "false";
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        return std::string(*value);
    } else {
        return std::to_string(*value);
    }
}

std::string renderTaggedElement(const ListElementView element);

std::string renderTaggedList(const ListView list) {
    std::string rendered = "[";
    for (size_t index = 0; const ListElementView element : list) {
        if (index > 0) {
            rendered += ", ";
        }

        rendered += renderTaggedElement(element);
        index++;
    }

    return rendered + "]";
}

std::string renderTaggedElement(const ListElementView element) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return std::to_string(element.getAs<int64_t>());
        break;

        case ListBufferTypeTag::Double:
            return std::to_string(element.getAs<double>());
        break;

        case ListBufferTypeTag::Bool:
            return static_cast<bool>(element.getAs<CustomBool>()) ? "true" : "false";
        break;

        case ListBufferTypeTag::String:
            return std::string(element.getAs<std::string_view>());
        break;

        case ListBufferTypeTag::ListView:
            return renderTaggedList(element.getAs<ListView>());
        break;

        case ListBufferTypeTag::Null:
            return "null";
        break;

        default:
            return "?";
        break;
    }
}

// Render one cell of a type-tagged scalar column - the column a heterogeneous UNWIND
// emits, whose cells need not share a type.
std::optional<std::string> renderTaggedCell(const Column* column, size_t row) {
    const auto* elements = dynamic_cast<const ColumnVector<ListElementView>*>(column);
    if (!elements) {
        return std::nullopt;
    }

    return renderTaggedElement((*elements)[row]);
}

// Render one output cell, whatever column shape the program emitted: a node ID from a
// scan, a nullable value from an unwound homogeneous list, or a tagged scalar from an
// unwound heterogeneous one.
std::string renderCell(const Column* column, size_t row) {
    if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
        return std::to_string((*nodeIDs)[row].getValue());
    }

    if (const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(column)) {
        return std::to_string((*counts)[row]);
    }

    if (const std::optional<std::string> integer = renderOptCell<int64_t>(column, row)) {
        return *integer;
    } else if (const std::optional<std::string> real = renderOptCell<double>(column, row)) {
        return *real;
    } else if (const std::optional<std::string> boolean = renderOptCell<CustomBool>(column, row)) {
        return *boolean;
    } else if (const std::optional<std::string> text = renderOptCell<std::string_view>(column, row)) {
        return *text;
    } else if (const std::optional<std::string> tagged = renderTaggedCell(column, row)) {
        return *tagged;
    }

    throw TuringException("CypherUnwindTest: unsupported output column type");
}

class CollectingRowSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            Row& row = _rows.emplace_back();
            for (const Column* column : chunks) {
                row.push_back(renderCell(column, rowIndex));
            }
        }
    }

    const Rows& rows() const { return _rows; }

private:
    Rows _rows;
};

}

// The Cypher frontend path for UNWIND of a literal list: parse, analyze and generate the
// db dialect, then lower and execute, so each test asserts the rows a query returns
// rather than the shape of the IR.
class CypherUnwindTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, Rows& rows) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.setV3();
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        CollectingRowSink sink;
        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, &sink, &memory);
        interpreter.run();

        rows = sink.rows();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        Rows actual;
        runQuery(query, actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(CypherUnwindTest, unwindsIntegerList) {
    // One row per element, in list order - the whole query is the unwind source.
    const Rows expected = {{"1"}, {"2"}, {"3"}};
    expectRows("UNWIND [1, 2, 3] AS x RETURN x", expected);
}

TEST_F(CypherUnwindTest, unwindsStringList) {
    const Rows expected = {{"one"}, {"two"}};
    expectRows("UNWIND ['one', 'two'] AS x RETURN x", expected);
}

TEST_F(CypherUnwindTest, unwindsDoubleList) {
    const Rows expected = {{std::to_string(1.5)}, {std::to_string(2.5)}};
    expectRows("UNWIND [1.5, 2.5] AS x RETURN x", expected);
}

TEST_F(CypherUnwindTest, unwindsBoolList) {
    const Rows expected = {{"true"}, {"false"}};
    expectRows("UNWIND [true, false] AS x RETURN x", expected);
}

TEST_F(CypherUnwindTest, unwindsHeterogeneousList) {
    // The elements share no type, so the column is type-erased and each cell keeps its
    // own tag.
    const Rows expected = {{"true"}, {"mixed"}, {"10"}};
    expectRows("UNWIND [true, 'mixed', 10] AS x RETURN x", expected);
}

TEST_F(CypherUnwindTest, unwindsMixedNumericListAsTaggedScalars) {
    // An integer and a float share no single type either, matching the legacy engine's
    // exact-type homogeneity rule: no promotion to a double column.
    const Rows expected = {{"1"}, {std::to_string(2.5)}};
    expectRows("UNWIND [1, 2.5] AS x RETURN x", expected);
}

TEST_F(CypherUnwindTest, unwindsEmptyListToNoRows) {
    const Rows expected = {};
    expectRows("UNWIND [] AS x RETURN x", expected);
}

TEST_F(CypherUnwindTest, crossesUnwindWithMatchedNodes) {
    // MATCH and UNWIND share no variable, so the two dataflows meet in a cross product:
    // every node paired with every element, the node repeated across its pair.
    // SimpleGraph holds 18 nodes, numbered 0 through 17.
    constexpr uint64_t simpleGraphNodeCount = 18;

    Rows expected;
    for (uint64_t nodeID = 0; nodeID < simpleGraphNodeCount; nodeID++) {
        expected.push_back({std::to_string(nodeID), "10"});
        expected.push_back({std::to_string(nodeID), "20"});
    }

    expectRows("MATCH (n) UNWIND [10, 20] AS x RETURN n, x", expected);
}

TEST_F(CypherUnwindTest, crossesHeterogeneousUnwindWithMatchedNodes) {
    // The heterogeneous sibling of crossesUnwindWithMatchedNodes: the unwound column is
    // type-erased, so the cross product broadcasts tagged scalars whose cells differ in
    // type. Every node pairs with all three elements, in list order.
    constexpr uint64_t simpleGraphNodeCount = 18;
    const Row elements = {"true", "mixed", "10"};

    Rows expected;
    for (uint64_t nodeID = 0; nodeID < simpleGraphNodeCount; nodeID++) {
        for (const std::string& element : elements) {
            expected.push_back({std::to_string(nodeID), element});
        }
    }

    expectRows("MATCH (n) UNWIND [true, 'mixed', 10] AS x RETURN n, x", expected);
}

TEST_F(CypherUnwindTest, crossesEmptyUnwindWithMatchedNodesToNoRows) {
    // An empty list pairs with nothing, so the whole query returns no row - it is not an
    // unsupported shape.
    const Rows expected = {};
    expectRows("MATCH (n) UNWIND [] AS x RETURN n, x", expected);
}

TEST_F(CypherUnwindTest, filtersOnUnwoundVariableInWhere) {
    // The unwound variable is one side of the WHERE predicate, so the filter runs over the
    // cross product of the nodes and the elements: a node survives only against the element
    // equal to its own age. In SimpleGraph only Remy (0) and Adam (1) carry an age, both
    // 32, so 99 keeps nobody and every ageless node compares null against both elements.
    const Rows expected = {{"Remy", "32"}, {"Adam", "32"}};
    expectRows("UNWIND [32, 99] AS wantedAge MATCH (n) WHERE n.age = wantedAge RETURN n.name, wantedAge", expected);
}

TEST_F(CypherUnwindTest, filtersOnUnwoundVariableInPropertyConstraint) {
    // The same filter written as an inline property constraint rather than a WHERE: the
    // unwound variable is the constraint's value, so the pattern is matched against a
    // value that varies per row instead of a literal. Same rows as the WHERE form.
    const Rows expected = {{"Remy", "32"}, {"Adam", "32"}};
    expectRows("UNWIND [32, 99] AS wantedAge MATCH (n {age: wantedAge}) RETURN n.name, wantedAge", expected);
}

TEST_F(CypherUnwindTest, sortsUnwoundList) {
    const Rows expected = {{"1"}, {"2"}, {"3"}};
    expectRows("UNWIND [3, 1, 2] AS l RETURN l ORDER BY l", expected);
}

TEST_F(CypherUnwindTest, unwindsNestedListElements) {
    // A nested list keeps its own elements as one cell, so the row is the inner list
    // itself rather than its elements.
    const Rows expected = {{"[1, 2]"}, {"[3]"}};
    expectRows("UNWIND [[1, 2], [3]] AS x RETURN x", expected);
}

TEST_F(CypherUnwindTest, unwindsNullListElements) {
    // A null has no type to share, so the list is type-erased and the null rides as a
    // cell of its own rather than dropping the row.
    const Rows expected = {{"1"}, {"null"}};
    expectRows("UNWIND [1, null] AS x RETURN x", expected);
}

TEST_F(CypherUnwindTest, unwindsListMixingScalarsWithNestedListAndNull) {
    const Rows expected = {{"1"}, {"true"}, {"hello"}, {std::to_string(3.14)}, {"[2]"}, {"null"}};
    expectRows("UNWIND [1, true, 'hello', 3.14, [2], null] AS l RETURN l", expected);
}

TEST_F(CypherUnwindTest, unwindsSingletonNullList) {
    const Rows expected = {{"null"}};
    expectRows("UNWIND [null] AS l RETURN l", expected);
}

TEST_F(CypherUnwindTest, sortsHeterogeneousUnwind) {
    // Cells of different types compare by the order their types sort in, following
    // Cypher's orderability: LIST < STRING < BOOLEAN < NUMBER.
    const Rows expected = {{"hello"}, {"true"}, {"1"}, {std::to_string(3.14)}};
    expectRows("UNWIND [1, true, 3.14, 'hello'] AS l RETURN l ORDER BY l", expected);
}

TEST_F(CypherUnwindTest, sortsHeterogeneousUnwindWithNullsLast) {
    // A null sorts after every value ascending, as it does in a nullable value column.
    const Rows expected = {{"[2]"}, {"a"}, {"1"}, {"null"}};
    expectRows("UNWIND [1, null, 'a', [2]] AS l RETURN l ORDER BY l", expected);
}

TEST_F(CypherUnwindTest, sortsHeterogeneousUnwindDescending) {
    const Rows expected = {{"null"}, {"1"}, {"true"}, {"hello"}};
    expectRows("UNWIND [1, true, 'hello', null] AS l RETURN l ORDER BY l DESC", expected);
}

TEST_F(CypherUnwindTest, limitsHeterogeneousUnwind) {
    const Rows expected = {{"1"}, {"null"}};
    expectRows("UNWIND [1, null, 'a'] AS l RETURN l LIMIT 2", expected);
}

TEST_F(CypherUnwindTest, skipsHeterogeneousUnwind) {
    const Rows expected = {{"null"}, {"a"}};
    expectRows("UNWIND [1, null, 'a'] AS l RETURN l SKIP 1", expected);
}

TEST_F(CypherUnwindTest, skipsSortedHeterogeneousUnwind) {
    // The skip cuts the sorted rows, not the list order: 'a' sorts first and is the row
    // dropped, leaving the number and the null.
    const Rows expected = {{"1"}, {"null"}};
    expectRows("UNWIND [1, null, 'a'] AS l RETURN l ORDER BY l SKIP 1", expected);
}

TEST_F(CypherUnwindTest, sortsHeterogeneousUnwindUnderTopK) {
    // ORDER BY fused with LIMIT keeps only the best k rows in the accumulator, so the
    // tagged cells are gathered and compacted rather than all held.
    const Rows expected = {{"[1]"}, {"b"}};
    expectRows("UNWIND [3, 'b', null, [1]] AS l RETURN l ORDER BY l LIMIT 2", expected);
}

TEST_F(CypherUnwindTest, sortsHeterogeneousUnwindUnderDescendingTopK) {
    const Rows expected = {{"null"}, {"3"}};
    expectRows("UNWIND [3, 'b', null, [1]] AS l RETURN l ORDER BY l DESC LIMIT 2", expected);
}

TEST_F(CypherUnwindTest, sortsMixedNumericTagsNumerically) {
    const Rows expected = {{std::to_string(1.5)}, {"2"}};
    expectRows("UNWIND [2, 1.5] AS l RETURN l ORDER BY l", expected);
}

TEST_F(CypherUnwindTest, sortsBooleansBeforeNumbers) {
    const Rows expected = {{"false"}, {"true"}, {"0"}, {"1"}};
    expectRows("UNWIND [true, 1, false, 0] AS l RETURN l ORDER BY l", expected);
}

TEST_F(CypherUnwindTest, sortsStringsWithNullLast) {
    const Rows expected = {{"a"}, {"b"}, {"null"}};
    expectRows("UNWIND ['b', 'a', null] AS l RETURN l ORDER BY l", expected);
}

TEST_F(CypherUnwindTest, sortsNestedListsLexicographically) {
    // Nested lists compare element-wise, and a list that is a prefix of another sorts
    // before it.
    const Rows expected = {{"[1]"}, {"[1, 2]"}, {"[1, 2, 3]"}};
    expectRows("UNWIND [[1, 2], [1], [1, 2, 3]] AS l RETURN l ORDER BY l", expected);
}

TEST_F(CypherUnwindTest, sortsNestedListsByElementType) {
    // The type order applies inside a nested list too: the string element sorts before
    // the number one, so the list holding it comes first.
    const Rows expected = {{"[1, a]"}, {"[1, 2]"}};
    expectRows("UNWIND [[1, 'a'], [1, 2]] AS l RETURN l ORDER BY l", expected);
}

TEST_F(CypherUnwindTest, sortsNestedListsWithNullElements) {
    const Rows expected = {{"[1, 2]"}, {"[1, null]"}};
    expectRows("UNWIND [[1, null], [1, 2]] AS l RETURN l ORDER BY l", expected);
}

TEST_F(CypherUnwindTest, sortsEmptyNestedListFirst) {
    const Rows expected = {{"[]"}, {"[1]"}, {"z"}};
    expectRows("UNWIND [[], [1], 'z'] AS l RETURN l ORDER BY l", expected);
}

TEST_F(CypherUnwindTest, sortsDoublyNestedLists) {
    const Rows expected = {{"[[1]]"}, {"[[2]]"}};
    expectRows("UNWIND [[[1]], [[2]]] AS l RETURN l ORDER BY l", expected);
}

TEST_F(CypherUnwindTest, sortsListsAndScalarsDescending) {
    const Rows expected = {{"null"}, {"a"}, {"[2]"}, {"[1]"}};
    expectRows("UNWIND [[2], [1], null, 'a'] AS l RETURN l ORDER BY l DESC", expected);
}

TEST_F(CypherUnwindTest, crossesUnwindOfNullAndNestedListWithMatchedNodes) {
    // The null and the nested list broadcast across the cross product like any other
    // tagged cell. SimpleGraph holds 18 nodes, numbered 0 through 17.
    constexpr uint64_t simpleGraphNodeCount = 18;
    const Row elements = {"null", "[1]"};

    Rows expected;
    for (uint64_t nodeID = 0; nodeID < simpleGraphNodeCount; nodeID++) {
        for (const std::string& element : elements) {
            expected.push_back({std::to_string(nodeID), element});
        }
    }

    expectRows("MATCH (n) UNWIND [null, [1]] AS l RETURN n, l", expected);
}

TEST_F(CypherUnwindTest, sortsUnwoundCellsBesideAMatchedProperty) {
    // The matched name rides along as a payload of the sort, which orders on the tagged
    // column alone.
    const Rows expected = {{"Remy", "null"}, {"Remy", "x"}, {"Remy", "[1]"}};
    expectRows("MATCH (n {name: 'Remy'}) UNWIND [null, [1], 'x'] AS l RETURN n.name, l ORDER BY l DESC", expected);
}

TEST_F(CypherUnwindTest, countsHomogeneousUnwind) {
    const Rows expected = {{"2"}};
    expectRows("UNWIND [1, 2] AS l RETURN count(l)", expected);
}

TEST_F(CypherUnwindTest, countsHeterogeneousUnwind) {
    // count charges the non-null cells, as it does the present values of a nullable
    // value column.
    const Rows expected = {{"2"}};
    expectRows("UNWIND [1, null, 'a'] AS l RETURN count(l)", expected);
}

TEST_F(CypherUnwindTest, countsNestedListElements) {
    const Rows expected = {{"2"}};
    expectRows("UNWIND [[1], null, 'a'] AS l RETURN count(l)", expected);
}

TEST_F(CypherUnwindTest, countsAllNullListAsZero) {
    const Rows expected = {{"0"}};
    expectRows("UNWIND [null, null] AS l RETURN count(l)", expected);
}

TEST_F(CypherUnwindTest, dedupsHeterogeneousUnwind) {
    // Every null is one value, so DISTINCT keeps a single null row.
    const Rows expected = {{"1"}, {"null"}};
    expectRows("UNWIND [1, null, 1, null] AS l RETURN DISTINCT l", expected);
}

TEST_F(CypherUnwindTest, dedupsNumbersAcrossTags) {
    // An integer and the float holding the same value are one Cypher value, so they
    // dedup together even though the cells are tagged differently.
    const Rows expected = {{"1"}, {"a"}};
    expectRows("UNWIND [1, 1.0, 'a', 'a'] AS l RETURN DISTINCT l", expected);
}

TEST_F(CypherUnwindTest, dedupsNestedLists) {
    const Rows expected = {{"[1]"}, {"[2]"}, {"null"}};
    expectRows("UNWIND [[1], [1], [2], null, null] AS l RETURN DISTINCT l", expected);
}

TEST_F(CypherUnwindTest, dedupsAndSortsHeterogeneousUnwind) {
    const Rows expected = {{"a"}, {"1"}, {"null"}};
    expectRows("UNWIND [1, null, 'a'] AS l RETURN DISTINCT l ORDER BY l", expected);
}

TEST_F(CypherUnwindTest, filtersOnHeterogeneousUnwoundVariableInWhere) {
    // The tagged cell compares against the property by the type it carries: the null
    // cell matches no row, where the integer one matches the ages equal to it.
    const Rows expected = {{"Remy", "32"}, {"Adam", "32"}};
    expectRows("UNWIND [32, null] AS l MATCH (n) WHERE n.age = l RETURN n.name, l", expected);
}

TEST_F(CypherUnwindTest, filtersOnHeterogeneousUnwoundVariableOnEitherSide) {
    const Rows expected = {{"Remy"}, {"Adam"}};
    expectRows("UNWIND [32, null] AS l MATCH (n) WHERE l = n.age RETURN n.name", expected);
}

TEST_F(CypherUnwindTest, filtersOnHeterogeneousUnwoundVariableAcrossNumericTags) {
    // A float cell equals an integer property of the same value, as one number equals
    // the other in Cypher.
    const Rows expected = {{"Remy"}, {"Adam"}};
    expectRows("UNWIND [32.0, null] AS l MATCH (n) WHERE n.age = l RETURN n.name", expected);
}

TEST_F(CypherUnwindTest, filtersOnHeterogeneousUnwoundVariableOfAnotherType) {
    // The string cell carries no integer, so it matches no age at all.
    const Rows expected = {{"Remy", "32"}, {"Adam", "32"}};
    expectRows("UNWIND [32, 'x'] AS l MATCH (n) WHERE n.age = l RETURN n.name, l", expected);
}

TEST_F(CypherUnwindTest, filtersOnHeterogeneousUnwoundStringVariable) {
    const Rows expected = {{"Remy", "Remy"}};
    expectRows("UNWIND ['Remy', null, 3] AS l MATCH (n) WHERE n.name = l RETURN n.name, l", expected);
}

