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

// The eight Person nodes of simpledb, by name ascending. A constant key holds no order, so
// most cases below only have to come back with this row set - the one keyed on the name has
// to come back with it in this order.
const Names personNames = {
    "Adam", "Cyrus", "Doruk", "Luc", "Martina", "Maxime", "Remy", "Suhas",
};

// Collects a projection that pairs a constant with a name. The constant column is a
// ColumnConst, which holds one value standing for every row rather than one per row, so
// it is read through the same subscript at each of them.
class OrderByConstantNameSink : public NLOutputSink {
public:
    OrderByConstantNameSink(size_t constantColumn, size_t nameColumn)
        : _constantColumn(constantColumn),
          _nameColumn(nameColumn)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* constants = dynamic_cast<const ColumnConst<int64_t>*>(chunks[_constantColumn]);
        ASSERT_NE(constants, nullptr);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[_nameColumn]);
        ASSERT_NE(names, nullptr);

        const auto& nameRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            ASSERT_TRUE(nameRaw[rowIndex].has_value());
            _rows.emplace_back((*constants)[rowIndex], std::string(*nameRaw[rowIndex]));
        }
    }

    const ConstantNameRows& rows() const { return _rows; }

    void sortedRows(ConstantNameRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    ConstantNameRows _rows;
    size_t _constantColumn {0};
    size_t _nameColumn {0};
};

}

// A constant ORDER BY key holds the same value in every row, so it changes no order and
// the sort is dropped: ORDER BY 1 sorts by nothing. An alias is only another spelling of
// the item it was given to, so a key naming the alias of a constant item is that same
// constant key - and dropping it is what keeps the constant column out of the sort, where
// it is read above the loop the other columns are read in.
class OrderByConstantAliasKeyTest : public TuringTest {
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

    // One row per name, each carrying the same constant
    void constantNameRowsFor(int64_t constant, const Names& names, ConstantNameRows& rows) {
        rows.clear();
        for (const std::string& name : names) {
            rows.emplace_back(constant, name);
        }
    }

    // The row set the query emits, compared order-independently: a constant key promises
    // the rows, not their order
    void expectConstantNameRowSet(std::string_view query,
                                  size_t constantColumn,
                                  size_t nameColumn,
                                  int64_t constant) {
        OrderByConstantNameSink sink(constantColumn, nameColumn);
        runQuery(query, &sink);

        ConstantNameRows expected;
        constantNameRowsFor(constant, personNames, expected);

        ConstantNameRows actual;
        sink.sortedRows(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    // The rows the query emits, in the order the sink saw them: here a key does order
    // them, so nothing sorts what it collected
    void expectConstantNameRows(std::string_view query,
                                size_t constantColumn,
                                size_t nameColumn,
                                int64_t constant,
                                const Names& names) {
        OrderByConstantNameSink sink(constantColumn, nameColumn);
        runQuery(query, &sink);

        ConstantNameRows expected;
        constantNameRowsFor(constant, names, expected);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// The key is the constant spelled out, so it is seen for what it is and no sort is
// emitted: this is the answer the two cases below have to reproduce.
TEST_F(OrderByConstantAliasKeyTest, spelledOutConstantKeyOrdersNothing) {
    expectConstantNameRowSet("MATCH (n:Person) RETURN 1 AS x, n.name ORDER BY 1", 0, 1, 1);
}

// The same key named through the alias of the constant item. Matched to that item instead
// of recognised as constant, it keys the sort on a column bound above the loop the name
// column is read in, and the collect that gathers them lands where the name does not
// reach it.
TEST_F(OrderByConstantAliasKeyTest, aliasOfAConstantKeyOrdersNothing) {
    expectConstantNameRowSet("MATCH (n:Person) RETURN 1 AS x, n.name ORDER BY x", 0, 1, 1);
}

// Which column the constant is changes nothing about what its alias names: the key is
// still constant, and the projection still comes back whole.
TEST_F(OrderByConstantAliasKeyTest, aliasOfATrailingConstantKeyOrdersNothing) {
    expectConstantNameRowSet("MATCH (n:Person) RETURN n.name, 1 AS x ORDER BY x", 1, 0, 1);
}

// A key that does order the rows, over a projection whose first column is a constant. The
// sort orders the column that carries the rows and the constant rides along, as it does
// past a dedup: keying on it would order nothing, and gathering it would anchor the sort
// above the loop the names are read in.
TEST_F(OrderByConstantAliasKeyTest, dynamicKeyOrdersPastALeadingConstant) {
    expectConstantNameRows("MATCH (n:Person) RETURN 1, n.name ORDER BY n.name", 0, 1, 1, personNames);
}

// LIMIT k cuts the sorted rows, so this is the first three names - and the cut is charged
// to the same column the sort ordered, the constant being one row repeated rather than k
// of them.
TEST_F(OrderByConstantAliasKeyTest, dynamicKeyOrdersPastALeadingConstantThenLimits) {
    const Names expected = {"Adam", "Cyrus", "Doruk"};
    expectConstantNameRows("MATCH (n:Person) RETURN 1, n.name ORDER BY n.name LIMIT 3", 0, 1, 1, expected);
}

// The SKIP sibling: the first six names of the same order are dropped, leaving the last
// two.
TEST_F(OrderByConstantAliasKeyTest, dynamicKeyOrdersPastALeadingConstantThenSkips) {
    const Names expected = {"Remy", "Suhas"};
    expectConstantNameRows("MATCH (n:Person) RETURN 1, n.name ORDER BY n.name SKIP 6", 0, 1, 1, expected);
}

// A cut with no ORDER BY, over the same projection: the constant is not what the rows are
// counted from, so the LIMIT is charged to the name column - where its truncate reaches
// the rows it cuts.
TEST_F(OrderByConstantAliasKeyTest, limitsPastALeadingConstant) {
    OrderByConstantNameSink sink(0, 1);
    runQuery("MATCH (n:Person) RETURN 1, n.name LIMIT 3", &sink);

    ConstantNameRows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows.size(), 3u);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
