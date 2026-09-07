#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
#include "iterators/ChunkConfig.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "StorageDialect.h"

#include "IRTestRows.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// The join, keyed on the embedding property.
const char* const joinOnVector = R"mlir(
func.func @main() {
  %0:4 = db.hash_join factor {
    %1 = db.scan_nodes() : !db.column<!storage.node_id>
    %2 = db.get_node_properties(%1, "vec") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %1, %2 : !db.column<!storage.node_id>, !db.column<none>
  } factor {
    %1 = db.scan_nodes() : !db.column<!storage.node_id>
    %2 = db.get_node_properties(%1, "vec") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %1, %2 : !db.column<!storage.node_id>, !db.column<none>
  } on 1, 1
  db.output(%0#0, %0#2) names ["n", "m"] : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The same equality as the nested loop the fusion replaces: the rows of the product the
// filter kept. What the join answers is read against this rather than against a
// hand-derived list, so the two forms are held to one another.
const char* const crossOnVector = R"mlir(
func.func @main() {
  %0:2 = db.cross_product factor {
    %1 = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %1 : !db.column<!storage.node_id>
  } factor {
    %1 = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %1 : !db.column<!storage.node_id>
  }
  %1 = db.get_node_properties(%0#0, "vec") : (!db.column<!storage.node_id>) -> !db.column<none>
  %2 = db.get_node_properties(%0#1, "vec") : (!db.column<!storage.node_id>) -> !db.column<none>
  %3 = db.eq %1, %2 : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
  %4:2 = db.filter(%3, {%0#0, %0#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%4#0, %4#1) names ["n", "m"] : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

// A db.hash_join keyed on an embedding property. Two embeddings are equal when they carry
// the same floats in the same order, so the join has to serialize the vector rather than
// turn the query away: `=` between two embedding columns is a comparison the engine makes.
class HashJoinEmbeddingKeyTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        buildVectorGraph();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Six nodes: 0 and 1 carry the same vector, 2 carries the same floats with the sign of
    // its zero flipped (-0.0 == 0.0, so it keys with them), 3 carries another vector, 4
    // carries a NaN - equal to nothing, itself included - and 5 carries no vector at all.
    void buildVectorGraph() {
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("Point");
        const PropertyTypeID vectorID = metadata.getOrCreatePropertyType("vec", ValueType::Embedding)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const std::vector<NodeID> nodes {builder.addNode(labelset),
                                         builder.addNode(labelset),
                                         builder.addNode(labelset),
                                         builder.addNode(labelset),
                                         builder.addNode(labelset),
                                         builder.addNode(labelset)};

        const float notANumber = std::numeric_limits<float>::quiet_NaN();
        const std::vector<std::vector<float>> vectors {{1.0f, 0.0f},
                                                       {1.0f, 0.0f},
                                                       {1.0f, -0.0f},
                                                       {2.0f, 5.0f},
                                                       {1.0f, notANumber}};

        for (size_t node = 0; node < vectors.size(); node++) {
            builder.addNodeProperty<types::Embedding>(nodes[node], vectorID, vectors[node]);
        }

        const auto submitResult = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitResult);
    }

    void runProgram(const char* programText, size_t chunkSize, RowSink& sink) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(dbModule);

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, chunkSize);
        interpreter.run();
    }

    void sortedRowsOf(const char* programText, size_t chunkSize, Rows& rows) {
        RowSink sink;
        runProgram(programText, chunkSize, sink);
        sink.sortedRows(rows);
    }

    const std::vector<size_t> _chunkSizes {1, 2, 4, ChunkConfig::CHUNK_SIZE};

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(HashJoinEmbeddingKeyTest, joinsTheRowsTheEqualityKeeps) {
    Rows expected;
    sortedRowsOf(crossOnVector, ChunkConfig::CHUNK_SIZE, expected);

    // 0, 1 and 2 hold one vector between them, 3 holds its own, and neither the NaN of 4
    // nor the absent vector of 5 matches anything.
    ASSERT_EQ(expected.size(), 10u);

    for (const size_t chunkSize : _chunkSizes) {
        Rows joined;
        sortedRowsOf(joinOnVector, chunkSize, joined);

        EXPECT_EQ(joined, expected) << "at chunk size " << chunkSize;
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
