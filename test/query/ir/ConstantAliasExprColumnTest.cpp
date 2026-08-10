#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBOps.h"
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
#include "columns/ColumnOptVector.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Names = std::vector<std::string>;
using ConstantNameRow = std::pair<int64_t, std::string>;
using ConstantNameRows = std::vector<ConstantNameRow>;

const Names personNames = {
    "Adam", "Cyrus", "Doruk", "Luc", "Martina", "Maxime", "Remy", "Suhas",
};

// Collects the computed constant and the name of each row. The computed column is a
// ColumnConst just as the constant it is computed from is: one value standing for every
// row, read through the same subscript at each of them.
class ComputedConstantNameSink : public NLOutputSink {
public:
    ComputedConstantNameSink(size_t computedColumn, size_t nameColumn)
        : _computedColumn(computedColumn),
          _nameColumn(nameColumn)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* computed = dynamic_cast<const ColumnConst<int64_t>*>(chunks[_computedColumn]);
        ASSERT_NE(computed, nullptr);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[_nameColumn]);
        ASSERT_NE(names, nullptr);

        const auto& nameRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            ASSERT_TRUE(nameRaw[rowIndex].has_value());
            _rows.emplace_back((*computed)[rowIndex], std::string(*nameRaw[rowIndex]));
        }
    }

    const ConstantNameRows& rows() const { return _rows; }

    void sortedRows(ConstantNameRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    ConstantNameRows _rows;
    size_t _computedColumn {0};
    size_t _nameColumn {0};
};

}

// An expression over the alias of a constant item - the x + 1 of RETURN 1 AS x, x + 1 -
// computes over constants alone, so it is one value standing for every row and is bound
// above the loop the row-carrying columns are read in. A symbol is marked dynamic whatever
// it names, so what a step shaped by rows may be handed has to be read off the emitted
// column rather than off the expression behind it.
class ConstantAliasExprColumnTest : public TuringTest {
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
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);

        LocalMemory memory;
        DBDialectInterpreter interpreter(moduleOp, &view, sink, &memory);
        interpreter.run();
    }

    void computedNameRowsFor(int64_t computed, const Names& names, ConstantNameRows& rows) {
        rows.clear();
        for (const std::string& name : names) {
            rows.emplace_back(computed, name);
        }
    }

    // The row set the query emits, compared order-independently
    void expectComputedNameRowSet(std::string_view query, int64_t computed) {
        ComputedConstantNameSink sink(1, 2);
        runQuery(query, &sink);

        ConstantNameRows expected;
        computedNameRowsFor(computed, personNames, expected);

        ConstantNameRows actual;
        sink.sortedRows(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    // The rows in the order the sink saw them, for a key that does order them
    void expectComputedNameRows(std::string_view query, int64_t computed, const Names& names) {
        ComputedConstantNameSink sink(1, 2);
        runQuery(query, &sink);

        ConstantNameRows expected;
        computedNameRowsFor(computed, names, expected);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// No step shaped by rows, so the projection alone: this is the answer every case below has
// to reproduce, the computed constant standing beside each name
TEST_F(ConstantAliasExprColumnTest, projectsAnExprOverAConstantAlias) {
    expectComputedNameRowSet("MATCH (n:Person) RETURN 1 AS x, x + 1, n.name", 2);
}

// The sort orders the name column, and the two constant columns ride along: keying on
// either would order nothing, and gathering one would anchor the sort above the loop the
// names are read in
TEST_F(ConstantAliasExprColumnTest, sortsPastAnExprOverAConstantAlias) {
    expectComputedNameRows("MATCH (n:Person) RETURN 1 AS x, x + 1, n.name ORDER BY n.name",
                           2,
                           personNames);
}

// The dedup keeps the rows the names distinguish, the constants riding along the same way
TEST_F(ConstantAliasExprColumnTest, dedupsPastAnExprOverAConstantAlias) {
    expectComputedNameRowSet("MATCH (n:Person) RETURN DISTINCT 1 AS x, x + 1, n.name", 2);
}

// The cut is charged to the column that carries the rows, the constants being one row
// repeated rather than three of them
TEST_F(ConstantAliasExprColumnTest, cutsPastAnExprOverAConstantAlias) {
    const Names expected = {"Adam", "Cyrus", "Doruk"};
    expectComputedNameRows("MATCH (n:Person) RETURN 1 AS x, x + 1, n.name ORDER BY n.name LIMIT 3",
                           2,
                           expected);
}

// A key of its own, computed over the alias of a constant and carried by no projected
// column: it is still one value for every row, so it orders nothing
TEST_F(ConstantAliasExprColumnTest, unprojectedExprOverAConstantAliasOrdersNothing) {
    expectComputedNameRowSet("MATCH (n:Person) RETURN 1 AS x, x + 1, n.name ORDER BY x * 3", 2);
}

// The alias is what makes the column look row-carrying: spelled out, the same arithmetic
// is constant to the analyzer too. Both spellings must reach the same answer.
TEST_F(ConstantAliasExprColumnTest, sortsPastASpelledOutConstantExpr) {
    expectComputedNameRows("MATCH (n:Person) RETURN 1 AS x, 1 + 1, n.name ORDER BY n.name",
                           2,
                           personNames);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
