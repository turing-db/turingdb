#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "metadata/GraphMetadata.h"
#include "metadata/LabelSetHandle.h"
#include "versioning/CommitJournal.h"
#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"
#include "writers/MetadataBuilder.h"

#include "DBDialect.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "DBDialectInterpreter.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Discards all output — CREATE-only programs have no meaningful rows to sink.
class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t) override {}
};

}

class CreateNodeEdgeTest : public TuringTest {
protected:
    // Parses and runs a db-dialect program against an empty graph with a live
    // write context. The CommitWriteBuffer accumulates pending nodes/edges so
    // the caller can inspect them without committing.
    void runCreateProgram(const char* programText) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule =
            mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(dbModule);

        NullSink sink;
        LocalMemory memory;

        DBDialectInterpreter interpreter(*dbModule, &_view, &sink, &memory,
                                        /*chunkSize=*/64,
                                        _writeBuffer.get(),
                                        _metadataBuilder.get());
        interpreter.run();
    }

    LabelID labelID(std::string_view name) {
        return _metadataBuilder->getOrCreateLabel(name);
    }

    EdgeTypeID edgeTypeID(std::string_view name) {
        return _metadataBuilder->getOrCreateEdgeType(name);
    }

    const CommitWriteBuffer& wb() const { return *_writeBuffer; }

    const CommitWriteBuffer::PendingNodes& pendingNodes() const {
        return wb().pendingNodes();
    }

    const CommitWriteBuffer::PendingEdges& pendingEdges() const {
        return wb().pendingEdges();
    }

    void initialize() override {
        _journal = CommitJournal::emptyJournal();

        GraphMetadata emptyPrev;
        _metadataBuilder = MetadataBuilder::create(emptyPrev, &_graphMetadata);

        _writeBuffer = std::make_unique<CommitWriteBuffer>(*_journal, _view);
    }

    GraphView _view;
    GraphMetadata _graphMetadata;
    std::unique_ptr<CommitJournal> _journal;
    std::unique_ptr<MetadataBuilder> _metadataBuilder;
    std::unique_ptr<CommitWriteBuffer> _writeBuffer;
};

// CREATE (n:Person)
TEST_F(CreateNodeEdgeTest, createSingleNode) {
    constexpr const char* program = R"mlir(
func.func @main() {
  %n = db.create_node (["Person"], [], {}) : () -> !db.column<!storage.node_id>
  return
}
)mlir";

    runCreateProgram(program);

    const LabelID personID = labelID("Person");

    ASSERT_EQ(_writeBuffer->numPendingNodes(), 1u);
    EXPECT_TRUE(pendingNodes()[0].labelsetHandle.hasLabel(personID));
    EXPECT_TRUE(pendingNodes()[0].properties.empty());

    EXPECT_EQ(_writeBuffer->numPendingEdges(), 0u);
}

// CREATE (a:Person)-[:KNOWS]->(b:Person)
TEST_F(CreateNodeEdgeTest, createEdgeBetweenNewNodes) {
    constexpr const char* program = R"mlir(
func.func @main() {
  %a = db.create_node (["Person"], [], {}) : () -> !db.column<!storage.node_id>
  %b = db.create_node (["Person"], [], {}) : () -> !db.column<!storage.node_id>
  %e = db.create_edge (%a, %b, "KNOWS", [], {})
         : (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
         -> !db.column<!storage.edge_id>
  return
}
)mlir";

    runCreateProgram(program);

    const LabelID personID = labelID("Person");
    const EdgeTypeID knowsID = edgeTypeID("KNOWS");

    ASSERT_EQ(_writeBuffer->numPendingNodes(), 2u);
    EXPECT_TRUE(pendingNodes()[0].labelsetHandle.hasLabel(personID));
    EXPECT_TRUE(pendingNodes()[1].labelsetHandle.hasLabel(personID));

    ASSERT_EQ(_writeBuffer->numPendingEdges(), 1u);

    const CommitWriteBuffer::PendingEdge& edge = pendingEdges()[0];
    EXPECT_EQ(edge.edgeType, knowsID);

    // Both endpoints must be pending offsets (not real NodeIDs)
    ASSERT_TRUE(std::holds_alternative<CommitWriteBuffer::PendingNodeOffset>(edge.src));
    ASSERT_TRUE(std::holds_alternative<CommitWriteBuffer::PendingNodeOffset>(edge.tgt));
    EXPECT_EQ(std::get<CommitWriteBuffer::PendingNodeOffset>(edge.src), 0u);
    EXPECT_EQ(std::get<CommitWriteBuffer::PendingNodeOffset>(edge.tgt), 1u);
}

// CREATE (n:Person {name: "Alice"})
TEST_F(CreateNodeEdgeTest, createNodeWithStringProperty) {
    constexpr const char* program = R"mlir(
func.func @main() {
  %name = db.constant("Alice" : !storage.string)
  %n = db.create_node (["Person"], ["name"], {%name})
         : (!db.column<!storage.string>) -> !db.column<!storage.node_id>
  return
}
)mlir";

    runCreateProgram(program);

    const LabelID personID = labelID("Person");

    ASSERT_EQ(_writeBuffer->numPendingNodes(), 1u);
    EXPECT_TRUE(pendingNodes()[0].labelsetHandle.hasLabel(personID));

    ASSERT_EQ(pendingNodes()[0].properties.size(), 1u);
    const CommitWriteBuffer::UntypedProperty& prop = pendingNodes()[0].properties[0];
    ASSERT_TRUE(std::holds_alternative<std::string>(prop.value));
    EXPECT_EQ(std::get<std::string>(prop.value), "Alice");
}
