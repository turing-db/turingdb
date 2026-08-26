#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"

#include "Graph.h"
#include "ProcedureContext.h"
#include "ProcedureManager.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListBuffer.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "SimpleGraph.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the rows of a projection whose first column is an edge and whose others are
// nodes, as the IDs they hold, in the order the projection names them.
class EdgeThenNodesSink : public NLOutputSink {
public:
    using Row = std::vector<uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const auto* edges = dynamic_cast<const ColumnVector<EdgeID>*>(chunks[0]);
        ASSERT_NE(edges, nullptr);

        std::vector<const ColumnVector<NodeID>*> nodeColumns;
        for (const Column* chunk : chunks.subspan(1)) {
            const auto* nodes = dynamic_cast<const ColumnVector<NodeID>*>(chunk);
            ASSERT_NE(nodes, nullptr);
            nodeColumns.push_back(nodes);
        }

        const auto& edgesRaw = edges->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            Row& row = _rows.emplace_back();
            row.push_back(edgesRaw[rowIndex].getValue());

            for (const ColumnVector<NodeID>* nodes : nodeColumns) {
                row.push_back(nodes->getRaw()[rowIndex].getValue());
            }
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

}

class CallYieldedEdgeIdentityTest : public TuringTest {
protected:
    void initialize() override {
        _procedures.init();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void runQuery(const char* queryText, NLOutputSink& sink) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(&_procedures, queryText);

        CypherParser parser(&ast);
        parser.parse(queryText);

        CypherAnalyzer analyzer(&ast, view);
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

        ProcedureContext procedureContext;
        procedureContext.setGraphView(&view);
        procedureContext.setProcedures(&_procedures);
        procedureContext.setChunkSize(ChunkConfig::CHUNK_SIZE);
        procedureContext.setListBuffer(&memory.listBuffer());

        DBDialectInterpreter interpreter(moduleOp,
                                         &view,
                                         &sink,
                                         &memory,
                                         ChunkConfig::CHUNK_SIZE,
                                         /*writeBuffer=*/nullptr,
                                         /*metadataBuilder=*/nullptr,
                                         &procedureContext);
        interpreter.run();
    }

    ProcedureManager _procedures;
    std::unique_ptr<Graph> _graph;
};

// CALL db.getEdges([0]) YIELD id AS e MATCH (x)-[e]->(y) RETURN e, x, y: the pattern reuses
// the yielded edge, so it binds to that edge alone - Remy to Adam - rather than scanning
// every edge beside it.
TEST_F(CallYieldedEdgeIdentityTest, patternReusesTheYieldedEdge) {
    EdgeThenNodesSink sink;
    runQuery("CALL db.getEdges([0]) YIELD id AS e MATCH (x)-[e]->(y) RETURN e, x, y", sink);

    std::vector<EdgeThenNodesSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<EdgeThenNodesSink::Row> expected {{0, 0, 1}};
    EXPECT_EQ(rows, expected);
}

// CALL db.getEdges([0]) YIELD id AS e, src, tgt MATCH (src)-[e]->(tgt) RETURN e, src, tgt:
// the pattern reuses the yielded edge and both of its endpoints, and the one row they
// describe survives every identity.
TEST_F(CallYieldedEdgeIdentityTest, patternReusesTheYieldedEdgeAndItsEndpoints) {
    EdgeThenNodesSink sink;
    runQuery("CALL db.getEdges([0]) YIELD id AS e, src, tgt MATCH (src)-[e]->(tgt) RETURN e, src, tgt", sink);

    std::vector<EdgeThenNodesSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<EdgeThenNodesSink::Row> expected {{0, 0, 1}};
    EXPECT_EQ(rows, expected);
}

// MATCH (n) CALL gnn.neighbourhoodSample(n, 1, 42) YIELD edge, src, tgt
// MATCH (x)-[edge]->(y) RETURN edge, src, tgt, x, y: a call driven per matched row yields
// an edge the pattern then reuses, so x and y are the endpoints the procedure reported for
// that very edge on every row.
TEST_F(CallYieldedEdgeIdentityTest, perRowCallYieldedEdgeBindsThePattern) {
    EdgeThenNodesSink sink;
    runQuery("MATCH (n) CALL gnn.neighbourhoodSample(n, 1, 42) YIELD edge, src, tgt "
             "MATCH (x)-[edge]->(y) RETURN edge, src, tgt, x, y",
             sink);

    std::vector<EdgeThenNodesSink::Row> rows;
    sink.sortedRows(rows);

    ASSERT_FALSE(rows.empty());
    for (const EdgeThenNodesSink::Row& row : rows) {
        EXPECT_EQ(row[3], row[1]);
        EXPECT_EQ(row[4], row[2]);
    }
}
