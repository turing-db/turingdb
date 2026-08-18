#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
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
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using ValueRow = std::vector<int64_t>;
using ValueRows = std::vector<ValueRow>;

// A constant a cut is charged to is laid out over the rows of the relation driving the
// projection, so a constant column reaches the sink as a value column wherever a MATCH
// makes its rows, and as a ColumnConst only when the projection is the one row it is.
int64_t readValue(const Column* column, size_t row) {
    if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
        return static_cast<int64_t>((*nodeIDs)[row].getValue());
    } else if (const auto* constants = dynamic_cast<const ColumnConst<int64_t>*>(column)) {
        return (*constants)[row];
    } else if (const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(column)) {
        const std::optional<int64_t>& value = values->getRaw()[row];
        if (!value) {
            throw std::runtime_error("LimitSkipTest: null row in a value output column");
        }

        return *value;
    }

    throw std::runtime_error("LimitSkipTest: unsupported output column type");
}

void describeRows(const ValueRows& rows, std::string& out) {
    out.clear();
    for (const ValueRow& row : rows) {
        out += "        {";
        for (size_t index = 0; index < row.size(); index++) {
            if (index > 0) {
                out += ", ";
            }

            out += std::to_string(row[index]);
        }

        out += "},\n";
    }
}

// Keeps the rows in the order the sink sees them: a SKIP and a LIMIT cut a window out of
// that order, and sorting them would hide which window came out.
class CollectingValueSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            ValueRow& row = _rows.emplace_back();
            for (const Column* column : chunks) {
                row.push_back(readValue(column, rowIndex));
            }
        }
    }

    const ValueRows& rows() const { return _rows; }

private:
    ValueRows _rows;
};

}

class LimitSkipTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, NLOutputSink* sink) {
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

        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, sink, &memory);
        interpreter.run();
    }

    void expectRows(std::string_view query, const ValueRows& expected) {
        CollectingValueSink sink;
        runQuery(query, &sink);

        const ValueRows& actual = sink.rows();

        std::string description;
        describeRows(actual, description);

        EXPECT_EQ(actual, expected)
            << "query: " << query << "\nactual rows:\n" << description;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(LimitSkipTest, matchLimitZeroEmitsNothing) {
    expectRows("MATCH (n) RETURN n LIMIT 0", {});
}

TEST_F(LimitSkipTest, matchSkipOneLimitOneEmitsTheSecondNode) {
    const ValueRows expected = {{1}};
    expectRows("MATCH (n) RETURN n SKIP 1 LIMIT 1", expected);
}

TEST_F(LimitSkipTest, matchSkipOneLimitZeroEmitsNothing) {
    expectRows("MATCH (n) RETURN n SKIP 1 LIMIT 0", {});
}

// A projection of constants alone is one row, and LIMIT 0 cuts it.
TEST_F(LimitSkipTest, constantLimitZeroEmitsNothing) {
    expectRows("RETURN 42 LIMIT 0", {});
}

TEST_F(LimitSkipTest, constantLimitOneEmitsTheRow) {
    const ValueRows expected = {{42}};
    expectRows("RETURN 42 LIMIT 1", expected);
}

// The one row is the row SKIP 1 drops, so the LIMIT has nothing left to keep.
TEST_F(LimitSkipTest, constantSkipOneLimitOneEmitsNothing) {
    expectRows("RETURN 42 SKIP 1 LIMIT 1", {});
}

// A constant beside a MATCH is one row per matched row, so the 18 nodes make 18 rows of
// it: the cut is charged to the rows the match makes and not to the single value the
// constant column holds.
TEST_F(LimitSkipTest, matchedConstantLimitKeepsTheLimitedRows) {
    const ValueRows expected = {{5}, {5}, {5}};
    expectRows("MATCH (n) RETURN 5 LIMIT 3", expected);
}

// The SKIP sibling: 18 rows minus the first 16 leaves two of them.
TEST_F(LimitSkipTest, matchedConstantSkipKeepsTheSurvivingRows) {
    const ValueRows expected = {{5}, {5}};
    expectRows("MATCH (n) RETURN 5 SKIP 16", expected);
}

TEST_F(LimitSkipTest, matchedConstantSkipLimitCutsAWindow) {
    const ValueRows expected = {{5}, {5}};
    expectRows("MATCH (n) RETURN 5 SKIP 1 LIMIT 2", expected);
}

// An expression over constants alone is a constant column too, and is cut the same way.
TEST_F(LimitSkipTest, matchedConstantExpressionLimitKeepsTheLimitedRows) {
    const ValueRows expected = {{42}, {42}, {42}};
    expectRows("MATCH (n) RETURN 40 + 2 LIMIT 3", expected);
}

// Every constant column is cut, not just the first one the cut is charged to.
TEST_F(LimitSkipTest, matchedConstantsLimitKeepsEveryColumn) {
    const ValueRows expected = {{5, 7}, {5, 7}, {5, 7}};
    expectRows("MATCH (n) RETURN 5, 7 LIMIT 3", expected);
}

// The cross product emits its 44 rows over several steps of a nest, so the budget is
// charged step by step rather than once for the whole projection.
TEST_F(LimitSkipTest, matchedConstantLimitAcrossANest) {
    const ValueRows expected = {{5}, {5}, {5}, {5}, {5}};
    expectRows("MATCH (a)-->(b), (a)-->(c) RETURN 5 LIMIT 5", expected);
}

// A limit of more rows than the match makes keeps all 18 of them.
TEST_F(LimitSkipTest, matchedConstantLimitPastTheMatchedRowsKeepsThemAll) {
    const ValueRows expected(18, ValueRow {5});
    expectRows("MATCH (n) RETURN 5 LIMIT 20", expected);
}

// A skip past every matched row leaves nothing, where charging the cut to the constant
// column would leave its one value behind.
TEST_F(LimitSkipTest, matchedConstantSkipPastTheMatchedRowsEmitsNothing) {
    expectRows("MATCH (n) RETURN 5 SKIP 20", {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
