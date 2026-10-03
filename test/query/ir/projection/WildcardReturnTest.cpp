#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
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
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Row = std::vector<uint64_t>;
using Rows = std::vector<Row>;

uint64_t readID(const Column* column, size_t row) {
    if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
        return (*nodeIDs)[row].getValue();
    } else if (const auto* edgeIDs = dynamic_cast<const ColumnEdgeIDs*>(column)) {
        return (*edgeIDs)[row].getValue();
    } else if (const auto* edgeTypes = dynamic_cast<const ColumnEdgeTypes*>(column)) {
        return (*edgeTypes)[row].getValue();
    }

    throw std::runtime_error("WildcardReturnTest: unsupported output column type");
}

void describeRows(const Rows& rows, std::string& out) {
    out.clear();
    for (const Row& row : rows) {
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

class CollectingIDSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        if (_columns.empty()) {
            _columns.resize(chunks.size());
        }

        ASSERT_EQ(chunks.size(), _columns.size());

        for (size_t columnIndex = 0; columnIndex < chunks.size(); columnIndex++) {
            const Column* column = chunks[columnIndex];

            for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
                _columns[columnIndex].push_back(readID(column, rowIndex));
            }
        }
    }

    void sortedRows(Rows& rows) const {
        rows.clear();
        const size_t rowCount = _columns.empty() ? 0 : _columns.front().size();

        for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            Row& row = rows.emplace_back();
            for (const std::vector<uint64_t>& column : _columns) {
                row.push_back(column[rowIndex]);
            }
        }

        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::vector<uint64_t>> _columns;
};

}

class WildcardReturnTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, Rows& rows) {
        SystemAccessor system = _env->getSystemManager().accessUnique();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(system.getProcedures(), query);

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

        CollectingIDSink sink;
        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, &sink, &memory);
        interpreter.run();

        sink.sortedRows(rows);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        Rows actual;
        runQuery(query, actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string description;
        describeRows(actual, description);

        EXPECT_EQ(actual, sortedExpected)
            << "query: " << query << "\nactual rows:\n" << description;
    }

    // RETURN * must produce the same columns, in the same order, as the explicit
    // enumeration it stands for. Comparing the two keeps the check independent of the
    // graph's edge ids
    void expectEquivalent(std::string_view wildcardQuery, std::string_view explicitQuery) {
        Rows wildcardRows;
        runQuery(wildcardQuery, wildcardRows);

        Rows explicitRows;
        runQuery(explicitQuery, explicitRows);

        std::string description;
        describeRows(wildcardRows, description);

        EXPECT_EQ(wildcardRows, explicitRows)
            << "wildcard: " << wildcardQuery << "\nexplicit: " << explicitQuery
            << "\nwildcard rows:\n" << description;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(WildcardReturnTest, singleNode) {
    const Rows expected = {{0}, {1},  {2},  {3},  {4},  {5},  {6},  {7},  {8},
                           {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}};
    expectRows("MATCH (n) RETURN *", expected);

    expectEquivalent("MATCH (n) RETURN *", "MATCH (n) RETURN n");
}

TEST_F(WildcardReturnTest, nodeEdgeNode) {
    expectEquivalent("MATCH (a)-[e]->(b) RETURN *",
                     "MATCH (a)-[e]->(b) RETURN a, e, b");
}

TEST_F(WildcardReturnTest, anonymousEdgeExcluded) {
    expectEquivalent("MATCH (a)-->(b) RETURN *",
                     "MATCH (a)-->(b) RETURN a, b");
}

TEST_F(WildcardReturnTest, mixedWithExplicitItem) {
    expectEquivalent("MATCH (a)-[e]->(b) RETURN *, a",
                     "MATCH (a)-[e]->(b) RETURN e, b, a");
}

TEST_F(WildcardReturnTest, mixedExplicitEdgeVar) {
    expectEquivalent("MATCH (a)-[e]->(b) RETURN *, e",
                     "MATCH (a)-[e]->(b) RETURN a, b, e");
}

TEST_F(WildcardReturnTest, mixedExplicitTrailingVarNotDuplicated) {
    expectEquivalent("MATCH (a)-[e]->(b) RETURN *, b",
                     "MATCH (a)-[e]->(b) RETURN a, e, b");
}

TEST_F(WildcardReturnTest, mixedTwoExplicitItems) {
    expectEquivalent("MATCH (a)-[e]->(b) RETURN *, a, b",
                     "MATCH (a)-[e]->(b) RETURN e, a, b");
}

TEST_F(WildcardReturnTest, mixedTwoExplicitItemsReordered) {
    expectEquivalent("MATCH (a)-[e]->(b) RETURN *, b, a",
                     "MATCH (a)-[e]->(b) RETURN e, b, a");
}

TEST_F(WildcardReturnTest, mixedDisconnectedPattern) {
    expectEquivalent("MATCH (a), (b) RETURN *, a",
                     "MATCH (a), (b) RETURN b, a");
}

TEST_F(WildcardReturnTest, withLimit) {
    expectEquivalent("MATCH (a)-[e]->(b) RETURN * LIMIT 5",
                     "MATCH (a)-[e]->(b) RETURN a, e, b LIMIT 5");
}
