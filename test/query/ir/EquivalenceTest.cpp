#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <range/v3/view/zip.hpp>

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
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Columns/dataframes are converted to strings row-wise for simple sorting and comparison
// code, but obviously slow. Since this is just a test and on a small graph (simpledb), I
// think the simplicity of the testing code is more valuable than an efficient
// implementation
using Row = std::vector<std::string>;
using Rows = std::vector<Row>;

template <typename T>
bool renderValueCell(const Column* column, size_t row, std::string& out) {
    const auto* values = dynamic_cast<const ColumnOptVector<T>*>(column);
    if (!values) {
        return false;
    }

    const std::optional<T> value = (*values)[row];
    if (!value) {
        out = "null";
        return true;
    }

    if constexpr (std::is_same_v<T, std::string_view>) {
        out = std::string(*value);
    } else {
        out = std::to_string(*value);
    }

    return true;
}

void renderCell(const Column* column, size_t row, std::string& out) {
    if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
        out = std::to_string((*nodeIDs)[row].getValue());
    } else if (const auto* edgeIDs = dynamic_cast<const ColumnEdgeIDs*>(column)) {
        out = std::to_string((*edgeIDs)[row].getValue());
    } else if (const auto* edgeTypes = dynamic_cast<const ColumnEdgeTypes*>(column)) {
        out = std::to_string((*edgeTypes)[row].getValue());
    } else if (renderValueCell<int64_t>(column, row, out)
               || renderValueCell<uint64_t>(column, row, out)
               || renderValueCell<double>(column, row, out)
               || renderValueCell<std::string_view>(column, row, out)) {
        // Rendered by the helper for whichever nullable value type matched
    } else {
        throw std::runtime_error("EquivalenceTest: unsupported output column type");
    }
}

void collectPipelineRows(const Dataframe* dataframe, Rows& rows) {
    const Dataframe::NamedColumns& columns = dataframe->cols();
    const size_t rowCount = dataframe->getLogicalRowCount();

    for (size_t row = 0; row < rowCount; row++) {
        Row& cells = rows.emplace_back();
        cells.resize(columns.size());

        for (size_t column = 0; column < columns.size(); column++) {
            renderCell(columns[column]->getColumn(), row, cells[column]);
        }
    }
}

class EquivalenceSink : public NLOutputSink {
public:
    explicit EquivalenceSink(Rows& rows)
        : _rows(rows)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t row = offset; row < offset + rowCount; row++) {
            Row& cells = _rows.emplace_back();
            cells.resize(chunks.size());

            for (size_t column = 0; column < chunks.size(); column++) {
                renderCell(chunks[column], row, cells[column]);
            }
        }
    }

private:
    Rows& _rows;
};

}

class EquivalenceTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);

        _db = &_env->getDB();
    }

    void runViaPipeline(std::string_view query, Rows& rows) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData([&rows](const Dataframe* dataframe) {
            ASSERT_TRUE(dataframe != nullptr);
            collectPipelineRows(dataframe, rows);
        });

        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks);
        const QueryStatus status = _db->query(query, state);
        ASSERT_TRUE(status.isOk()) << "Pipeline query failed: " << query;
    }

    void runViaIR(std::string_view query, Rows& rows) {
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

        EquivalenceSink sink(rows);
        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, &sink, &memory);
        interpreter.run();
    }

    void expectEquivalent(std::string_view query) {
        Rows pipelineRows;
        runViaPipeline(query, pipelineRows);

        Rows irRows;
        runViaIR(query, irRows);

        std::ranges::sort(pipelineRows);
        std::ranges::sort(irRows);

        ASSERT_EQ(pipelineRows.size(), irRows.size()) << "Size mismatch for " << query;

        for (auto [plRow, irRow] : rv::zip(pipelineRows, irRows)) {
            EXPECT_EQ(plRow, irRow) << "Row mismatch for " << query;
        }
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    QueryConfig _queryConfig;
};

TEST_F(EquivalenceTest, scans) {
    expectEquivalent("MATCH (n) RETURN n");
}

TEST_F(EquivalenceTest, outEdges) {
    expectEquivalent("MATCH (a)-->(b) RETURN a");
    expectEquivalent("MATCH (a)-->(b) RETURN b");
    expectEquivalent("MATCH (a)-->(b) RETURN a, b");

    expectEquivalent("MATCH (a)-->(b)-->(c) RETURN a");
    expectEquivalent("MATCH (a)-->(b)-->(c) RETURN b");
    expectEquivalent("MATCH (a)-->(b)-->(c) RETURN c");
    expectEquivalent("MATCH (a)-->(b)-->(c) RETURN a, b, c");
    expectEquivalent("MATCH (a)-->(b)-->(c) RETURN a, c");

    expectEquivalent("MATCH (a)-->(b)-->(c)-->(d) RETURN a, b, c, d");
    expectEquivalent("MATCH (a)-->(b)-->(c)-->(d) RETURN b, c");
    expectEquivalent("MATCH (a)-->(b)-->(c)-->(d) RETURN a, d");
}

TEST_F(EquivalenceTest, inEdges) {
    expectEquivalent("MATCH (a)<--(b) RETURN a");
    expectEquivalent("MATCH (a)<--(b) RETURN b");
    expectEquivalent("MATCH (a)<--(b) RETURN a, b");

    expectEquivalent("MATCH (a)<--(b)<--(c) RETURN a");
    expectEquivalent("MATCH (a)<--(b)<--(c) RETURN b");
    expectEquivalent("MATCH (a)<--(b)<--(c) RETURN c");
    expectEquivalent("MATCH (a)<--(b)<--(c) RETURN a, b, c");
    expectEquivalent("MATCH (a)<--(b)<--(c) RETURN a, c");

    expectEquivalent("MATCH (a)<--(b)<--(c)<--(d) RETURN a, b, c, d");
    expectEquivalent("MATCH (a)<--(b)<--(c)<--(d) RETURN b, c");
    expectEquivalent("MATCH (a)<--(b)<--(c)<--(d) RETURN a, d");
}

TEST_F(EquivalenceTest, trees) {
    expectEquivalent("MATCH (a)-->(b), (a)-->(c) RETURN a");
    expectEquivalent("MATCH (a)-->(b), (a)-->(c)-->(d) RETURN a");
    expectEquivalent("MATCH (b)-->(a), (c)-->(a) RETURN c");
    // Disabled due to suspected v2 bug
    // expectEquivalent("MATCH (a)-->(b)-->(c), (b)-->(e) RETURN a");
}

TEST_F(EquivalenceTest, crossProd) {
    expectEquivalent("MATCH (a), (b) RETURN a");
    expectEquivalent("MATCH (a), (b) RETURN b");
    expectEquivalent("MATCH (a), (b) RETURN a, b");

    expectEquivalent("MATCH (a), (b), (c) RETURN b");
    expectEquivalent("MATCH (a), (b), (c) RETURN a, b, c");

    expectEquivalent("MATCH (a)-->(d), (b), (c) RETURN a, b, c, d");

    expectEquivalent("MATCH (a)-->(d), (b)-->(e), (c)-->(f) RETURN a, b, c, d, e, f");
}

TEST_F(EquivalenceTest, eqFilters) {
    expectEquivalent("MATCH (a) WHERE a.age = 32 RETURN a");
    expectEquivalent("MATCH (a), (b) WHERE a.age = b.age RETURN a, b");
    expectEquivalent("MATCH (a), (b) WHERE a.name = a.name RETURN b");

    expectEquivalent("MATCH (a), (b) WHERE a = b RETURN b");
    expectEquivalent("MATCH (a), (b) WHERE a = a RETURN a");
    expectEquivalent("MATCH (a), (b) WHERE a = a RETURN b");

    expectEquivalent("MATCH (a), (b) WHERE a.age = a RETURN b");

    expectEquivalent("MATCH (a), (b) WHERE a = b.age RETURN a, b");

    expectEquivalent("MATCH (a), (b) WHERE a.age = 32 OR a.name = b.name RETURN b");
    expectEquivalent("MATCH (a), (b) WHERE a.age = 32 AND a.name = b.name RETURN b");

    expectEquivalent("MATCH (n)-->(a)-->(m) WHERE n = m RETURN n, a, m");

    // Disabled due to suspected v2 bug
    // expectEquivalent("MATCH (n)-->(a)-->(m), (a)-->(b) WHERE b = n RETURN n, a, m, b");
}
