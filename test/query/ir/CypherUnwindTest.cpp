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

// Render one cell of a type-tagged scalar column - the column a heterogeneous UNWIND
// emits, whose cells need not share a type.
std::optional<std::string> renderTaggedCell(const Column* column, size_t row) {
    const auto* elements = dynamic_cast<const ColumnVector<ListElementView>*>(column);
    if (!elements) {
        return std::nullopt;
    }

    const ListElementView element = (*elements)[row];
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

        default:
            return "?";
        break;
    }
}

// Render one output cell, whatever column shape the program emitted: a node ID from a
// scan, a nullable value from an unwound homogeneous list, or a tagged scalar from an
// unwound heterogeneous one.
std::string renderCell(const Column* column, size_t row) {
    if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
        return std::to_string((*nodeIDs)[row].getValue());
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

TEST_F(CypherUnwindTest, rejectsNestedListElements) {
    // A nested list has no tagged scalar form, so codegen refuses it rather than
    // emitting IR the interpreter cannot materialize.
    Rows rows;
    EXPECT_THROW(runQuery("UNWIND [[1, 2], [3]] AS x RETURN x", rows), TuringException);
}

TEST_F(CypherUnwindTest, rejectsNullListElements) {
    Rows rows;
    EXPECT_THROW(runQuery("UNWIND [1, null] AS x RETURN x", rows), TuringException);
}
