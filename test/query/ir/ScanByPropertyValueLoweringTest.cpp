#include <gtest/gtest.h>

#include <stdint.h>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
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
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr size_t nodeCount = 6;

class CollectingNodeSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const ColumnNodeIDs* const nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        ASSERT_NE(nodeIDs, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _nodeIDs.push_back((*nodeIDs)[rowIndex].getValue());
        }
    }

    const std::vector<uint64_t>& getNodeIDs() const { return _nodeIDs; }

private:
    std::vector<uint64_t> _nodeIDs;
};

std::string scanProgram(const std::string& literal) {
    return "func.func @main() {\n"
           "  %n = db.scan_nodes_by_property_value(" + literal + ") : !db.column<!storage.node_id>\n"
           "  db.output(%n) : !db.column<!storage.node_id>\n"
           "  return\n"
           "}\n";
}

}

// The literal kinds db.scan_nodes_by_property_value admits, run end to end against a
// graph carrying one property of every scannable type. The analyzer turns a double
// equality away before codegen, so the f64 cases are only reachable from written IR.
class ScanByPropertyValueLoweringTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();
        MetadataBuilder& metadata = builder.getMetadata();

        const LabelID rowLabel = metadata.getOrCreateLabel("Row");
        const PropertyTypeID valueID = metadata.getOrCreatePropertyType("value", ValueType::Int64)._id;
        const PropertyTypeID countID = metadata.getOrCreatePropertyType("count", ValueType::UInt64)._id;
        const PropertyTypeID ratioID = metadata.getOrCreatePropertyType("ratio", ValueType::Double)._id;
        const PropertyTypeID flagID = metadata.getOrCreatePropertyType("flag", ValueType::Bool)._id;
        const PropertyTypeID tagID = metadata.getOrCreatePropertyType("tag", ValueType::String)._id;

        const LabelSet labelset = LabelSet::fromList({rowLabel});

        for (size_t row = 0; row < nodeCount; row++) {
            const NodeID nodeID = builder.addNode(labelset);
            _nodeIDs.push_back(nodeID.getValue());

            builder.addNodeProperty<types::Int64>(nodeID, valueID, static_cast<int64_t>(row));
            builder.addNodeProperty<types::UInt64>(nodeID, countID, row);
            builder.addNodeProperty<types::Double>(nodeID, ratioID, static_cast<double>(row) / 2.0);
            builder.addNodeProperty<types::Bool>(nodeID, flagID, CustomBool(row % 2 == 0));

            const std::string tag = "t" + std::to_string(row % 2);
            builder.addNodeProperty<types::String>(nodeID, tagID, tag);
        }

        ASSERT_TRUE(change->access().submit(*_jobSystem));
    }

    // Lowers a db program to nl and runs it, filling `found` with the node IDs it emits.
    void scan(const std::string& literal, std::vector<uint64_t>& found) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const std::string programText = scanProgram(literal);
        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(dbModule) << programText;

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view);
        lowering.lower(dbFunction, *nlModule);

        CollectingNodeSink sink;
        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();

        found = sink.getNodeIDs();
    }

    void expectRows(const std::string& literal, const std::vector<size_t>& rows) {
        std::vector<uint64_t> expected;
        for (const size_t row : rows) {
            expected.push_back(_nodeIDs[row]);
        }

        std::vector<uint64_t> found;
        scan(literal, found);
        EXPECT_EQ(found, expected) << literal;
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    std::vector<uint64_t> _nodeIDs;
};

TEST_F(ScanByPropertyValueLoweringTest, scansADoublePropertyAgainstAnF64Literal) {
    expectRows("\"ratio\", 1.500000e+00 : f64", {3});
    expectRows("\"ratio\", 0.000000e+00 : f64", {0});
}

TEST_F(ScanByPropertyValueLoweringTest, aDoublePropertyDoesNotMatchAnIntegerLiteral) {
    expectRows("\"ratio\", 1 : i64", {});
}

TEST_F(ScanByPropertyValueLoweringTest, scansAnUnsignedPropertyAgainstANonNegativeLiteral) {
    expectRows("\"count\", 4 : i64", {4});
}

TEST_F(ScanByPropertyValueLoweringTest, anUnsignedPropertyDoesNotMatchANegativeLiteral) {
    expectRows("\"count\", -1 : i64", {});
}

TEST_F(ScanByPropertyValueLoweringTest, scansTheOtherStoredTypesAgainstTheirOwnLiteralKind) {
    expectRows("\"value\", 2 : i64", {2});
    expectRows("\"flag\", true", {0, 2, 4});
    expectRows("\"tag\", \"t1\" : !storage.string", {1, 3, 5});
}

TEST_F(ScanByPropertyValueLoweringTest, aLiteralOfAnotherKindMatchesNothing) {
    expectRows("\"tag\", 3 : i64", {});
    expectRows("\"value\", \"2\" : !storage.string", {});
    expectRows("\"flag\", 1 : i64", {});
}

TEST_F(ScanByPropertyValueLoweringTest, aPropertyAbsentFromTheSchemaMatchesNothing) {
    expectRows("\"missing\", 1 : i64", {});
}

TEST_F(ScanByPropertyValueLoweringTest, aLabelledScanNarrowsToTheLabelledNodes) {
    expectRows("\"value\", 2 : i64, [\"Row\"]", {2});
    expectRows("\"value\", 2 : i64, [\"Nope\"]", {});
}
