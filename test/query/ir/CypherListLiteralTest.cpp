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
#include "columns/ColumnConst.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
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

std::string renderList(ListView list);

// Render one element of a list as its value, recursing into a nested list.
std::string renderListElement(const ListElementView element) {
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
            return renderList(element.getAs<ListView>());
        break;

        case ListBufferTypeTag::Null:
            return "null";
        break;

        default:
            return "?";
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

// Render one output cell, whatever column shape the program emitted: the one list a
// literal holds in every row, a node ID from a scan, or a property value.
std::string renderCell(const Column* column, size_t row) {
    if (const auto* lists = dynamic_cast<const ColumnConst<ListView>*>(column)) {
        return renderList((*lists)[row]);
    }

    if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
        return std::to_string((*nodeIDs)[row].getValue());
    }

    if (const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(column)) {
        const std::optional<std::string_view>& name = (*names)[row];
        return name ? std::string(*name) : "null";
    }

    throw TuringException("CypherListLiteralTest: unsupported output column type");
}

// The elements of a list long enough to outgrow one ListBuffer chunk, comma-separated -
// the spelling both the query literal and the rendered cell use. A chunk holds 4096 bytes
// and an integer element costs a one-byte tag beside its eight bytes, so 456 of them no
// longer fit one and the buffer must reserve a fresh chunk for the whole run.
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

// The Cypher frontend path for a list literal: parse, analyze and generate the db
// dialect, then lower and execute, so each test asserts the rows a query returns rather
// than the shape of the IR. A list literal is a value, not a source: the whole list is
// one cell, standing for every row - where UNWIND of the same list would spread it over
// one row per element.
class CypherListLiteralTest : public TuringTest {
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

TEST_F(CypherListLiteralTest, returnsIntegerList) {
    // The list is the whole projection and reads no row, so it is one row holding it.
    const Rows expected = {{"[1, 2, 3]"}};
    expectRows("RETURN [1, 2, 3]", expected);
}

TEST_F(CypherListLiteralTest, returnsFourElementIntegerList) {
    // The length is the literal array's, not a shape the op fixes: one element more than
    // the case above is one cell more in the same run.
    const Rows expected = {{"[1, 2, 3, 4]"}};
    expectRows("RETURN [1, 2, 3, 4]", expected);
}

TEST_F(CypherListLiteralTest, returnsListLongerThanOneBufferChunk) {
    const std::string elements = longListElements();
    const Rows expected = {{"[" + elements + "]"}};

    expectRows("RETURN [" + elements + "]", expected);
}

TEST_F(CypherListLiteralTest, keepsAnEarlierListWholePastAChunkAllocation) {
    // The second list is written once the first has forced a fresh chunk, so the views the
    // first handed out have to survive that allocation - the stability the ListBuffer
    // promises. Both cells still read back whole.
    const std::string elements = longListElements();
    const Rows expected = {{"[" + elements + "]", "[1, 2, 3]"}};

    expectRows("RETURN [" + elements + "], [1, 2, 3]", expected);
}

TEST_F(CypherListLiteralTest, returnsStringList) {
    const Rows expected = {{"[one, two]"}};
    expectRows("RETURN ['one', 'two']", expected);
}

TEST_F(CypherListLiteralTest, returnsHeterogeneousList) {
    // The elements share no type, so the list is type-erased - each element keeps its own
    // tag, which is what the cell renders.
    const Rows expected = {{"[true, mixed, 10]"}};
    expectRows("RETURN [true, 'mixed', 10]", expected);
}

TEST_F(CypherListLiteralTest, returnsEmptyList) {
    // An empty list is a value like any other: one row holding no element - unlike UNWIND
    // of an empty list, which yields no row at all.
    const Rows expected = {{"[]"}};
    expectRows("RETURN []", expected);
}

TEST_F(CypherListLiteralTest, returnsNestedList) {
    const Row row = {"[10, true, [deep, " + std::to_string(2.5) + "]]"};
    const Rows expected = {row};
    expectRows("RETURN [10, true, ['deep', 2.5]]", expected);
}

TEST_F(CypherListLiteralTest, returnsNestedEmptyList) {
    const Rows expected = {{"[[]]"}};
    expectRows("RETURN [[]]", expected);
}

TEST_F(CypherListLiteralTest, returnsTheListBesideAMatchedRow) {
    // One node is named Remy, so the projection is that one row and the list stands for
    // it: the row count is the name column's.
    const Rows expected = {{"[1, 2]", "Remy"}};
    expectRows("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 2], n.name", expected);
}

TEST_F(CypherListLiteralTest, repeatsTheListForEveryMatchedRow) {
    // A projection of the list alone has no per-row column, yet its cardinality is the
    // driving relation's: one row per matched node, each holding the same list.
    // SimpleGraph holds 18 nodes.
    constexpr size_t simpleGraphNodeCount = 18;
    const Rows expected(simpleGraphNodeCount, Row {"[1, 2]"});

    expectRows("MATCH (n) RETURN [1, 2]", expected);
}

TEST_F(CypherListLiteralTest, emitsNothingWhenTheMatchIsEmpty) {
    // No node carries that name, so the step keeps no row. The list holds its one value
    // all the same - it is not a row, and counting it as one would answer a query that
    // matched nothing with a row.
    const Rows expected = {};
    expectRows("MATCH (n) WHERE n.name = 'nobody' RETURN [1, 2], n.name", expected);
}

TEST_F(CypherListLiteralTest, holdsNullListElements) {
    const Rows expected = {{"[1, null]"}};
    expectRows("RETURN [1, null]", expected);
}

TEST_F(CypherListLiteralTest, holdsASingletonNullList) {
    const Rows expected = {{"[null]"}};
    expectRows("RETURN [null]", expected);
}

TEST_F(CypherListLiteralTest, rejectsMapListElements) {
    Rows rows;
    EXPECT_THROW(runQuery("RETURN [{age: 32}]", rows), TuringException);
}
