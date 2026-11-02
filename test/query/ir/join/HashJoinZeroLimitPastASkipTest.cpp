#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "llvm/Support/raw_ostream.h"

#include "Graph.h"
#include "JobSystem.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "CypherAnalyzer.h"
#include "CypherAST.h"
#include "CypherParser.h"

#include "DBDialect.h"
#include "DBPasses.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

// 400 named nodes make 160000 pairs, which the cost model fuses into a hash join unless a
// limit lets the product stop early. A limit of zero rows stops the product's loops before
// their first step, whatever skip sits in front of it.
class HashJoinZeroLimitPastASkipTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        buildGraph();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void buildGraph() {
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelID pointLabel = metadata.getOrCreateLabel("Point");
        const PropertyTypeID nameID = metadata.getOrCreatePropertyType("name", ValueType::String)._id;
        const LabelSet pointLabelSet = LabelSet::fromList({pointLabel});

        for (size_t node = 0; node < NODE_COUNT; node++) {
            const NodeID nodeID = builder.addNode(pointLabelSet);
            builder.addNodeProperty<types::String>(nodeID, nameID, "point" + std::to_string(node));
        }

        const auto submitResult = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitResult);
    }

    bool fuses(std::string_view query) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();
        const mlir::db::DBPassContext context {&view, false, true};

        CypherAST ast(nullptr, query);
        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::MLIRContext mlirContext;
        mlirContext.getOrLoadDialect<mlir::func::FuncDialect>();
        mlirContext.getOrLoadDialect<mlir::storage::Storage>();
        mlirContext.getOrLoadDialect<mlir::db::DB>();
        mlirContext.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&mlirContext);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module, nullptr, context);
        generator.generate(&ast);

        std::string program;
        llvm::raw_string_ostream stream(program);
        module.print(stream);

        return program.find("db.hash_join") != std::string::npos;
    }

    static constexpr size_t NODE_COUNT = 400;

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(HashJoinZeroLimitPastASkipTest, keepsTheProductUnderASmallSkip) {
    EXPECT_FALSE(fuses("MATCH (n), (m) WHERE n.name = m.name RETURN n, m SKIP 5 LIMIT 0"));
    EXPECT_FALSE(fuses("MATCH (n), (m) WHERE n.name = m.name WITH n, m SKIP 5 LIMIT 0 RETURN count(*)"));
}

TEST_F(HashJoinZeroLimitPastASkipTest, keepsTheProductUnderASkipPastTheSmallLimitBound) {
    EXPECT_FALSE(fuses("MATCH (n), (m) WHERE n.name = m.name RETURN n, m SKIP 20000 LIMIT 0"));
}

// A LIMIT 1 alone keeps the product (400 > 1 * 200), and the 5 skipped rows lift its budget
// to 6, which the join repays (400 <= 6 * 200).
TEST_F(HashJoinZeroLimitPastASkipTest, fusesWhenASkipLiftsALimitOfOne) {
    EXPECT_TRUE(fuses("MATCH (n), (m) WHERE n.name = m.name RETURN n, m SKIP 5 LIMIT 1"));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
