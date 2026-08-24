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
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Counts = std::vector<uint64_t>;
using Row = std::vector<std::string>;
using Rows = std::vector<Row>;

class ScalarCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[0]);
        ASSERT_NE(counts, nullptr);

        const auto& raw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const Counts& values() const { return _values; }

private:
    Counts _values;
};

class NameRowSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            Row& row = _rows.emplace_back();
            for (const Column* const column : chunks) {
                const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(column);
                if (!names) {
                    throw TuringException("CascadedMergeJoinTest: expected a string property column");
                }

                const std::optional<std::string_view>& name = (*names)[rowIndex];
                row.push_back(name ? std::string(*name) : "null");
            }
        }
    }

    const Rows& rows() const { return _rows; }

private:
    Rows _rows;
};

}

// Several patterns joining the same pair of nodes, through the MLIR frontend: each query
// is parsed, analyzed, generated into the db dialect, then lowered and interpreted.
//
// Every extra pattern between the same two nodes adds a cycle the dependency graph breaks
// with a merge edge, and once a node collects more than two of them they are cascaded
// through intermediate merge nodes. A merge edge joins two dataflows rather than
// traversing the graph, so it is never half of a (source, edge, target) triple: reading
// one as such made a three-pattern query fail in codegen instead of running.
class CascadedMergeJoinTest : public TuringTest {
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
        DBDialectInterpreter interpreter(module, &view, sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();
    }

    void expectScalarCount(std::string_view query, uint64_t expected) {
        ScalarCountSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.values(), (Counts {expected})) << "query: " << query;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        NameRowSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// The reported query. SimpleGraph holds three KNOWS_WELL edges - Remy -> Adam,
// Adam -> Remy and Ghosts -> Remy - and no two nodes carry more than one edge between
// them, so e1 and e2 each have the single edge of the pair to bind and the three patterns
// match one row per KNOWS_WELL edge.
TEST_F(CascadedMergeJoinTest, countsThreePatternsOnOneNodePair) {
    expectScalarCount("MATCH (a)-[:KNOWS_WELL]->(b), (a)-[e1]->(b), (a)-[e2]->(b) RETURN count(*)", 3);
}

// The merges constrain the pair, so both edge variables land on the pair's own edge -
// the KNOWS_WELL edge the first pattern matched.
TEST_F(CascadedMergeJoinTest, bindsEveryEdgeVariableToTheEdgeOfThePair) {
    const Rows knownPairs = {
        {"Remy", "Adam", "Remy -> Adam", "Remy -> Adam"},
        {"Adam", "Remy", "Adam -> Remy", "Adam -> Remy"},
        {"Ghosts", "Remy", "Ghosts -> Remy", "Ghosts -> Remy"},
    };

    expectRows("MATCH (a)-[:KNOWS_WELL]->(b), (a)-[e1]->(b), (a)-[e2]->(b) "
               "RETURN a.name, b.name, e1.name, e2.name",
               knownPairs);
}

// A fourth pattern cascades the merges one level deeper without widening the match.
TEST_F(CascadedMergeJoinTest, countsFourPatternsOnOneNodePair) {
    expectScalarCount("MATCH (a)-[:KNOWS_WELL]->(b), (a)-[e1]->(b), (a)-[e2]->(b), (a)-[e3]->(b) "
                      "RETURN count(*)",
                      3);
}

// Unconstrained, the patterns collapse onto one edge each, so the match is every edge of
// the graph: SimpleGraph has 18.
TEST_F(CascadedMergeJoinTest, countsThreeUntypedPatternsOnOneNodePair) {
    expectScalarCount("MATCH (a)-[e1]->(b), (a)-[e2]->(b), (a)-[e3]->(b) RETURN count(*)", 18);
}

// Traversing on from the merged pair: e3 leaves b once the two patterns have merged onto
// it, so the match is the two-hop chain, which MATCH (a)-[e1]->(b)-[e3]->(c) also counts.
TEST_F(CascadedMergeJoinTest, traversesOnFromTheMergedPair) {
    expectScalarCount("MATCH (a)-[e1]->(b), (a)-[e2]->(b), (b)-[e3]->(c) RETURN count(*)", 12);
    expectScalarCount("MATCH (a)-[e1]->(b)-[e3]->(c) RETURN count(*)", 12);
}
