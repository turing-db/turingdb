#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

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

// Which of the two forms a cross product cut by an equality is left in. The join reads its
// whole build side and indexes every key before it emits a row, so it only repays itself
// on a product large enough - and on one a limit does not cut short. This is the v2
// planner's cost model (ReadStmtGenerator::shouldPlaceValueHashJoin over
// CardinalityEstimation), reading the same node counts on the v3 pipeline.
class HashJoinCostModelTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // @param nodeCount nodes labelled Point, the first @param rareCount of them labelled
    // Rare as well, so a query naming Rare scans a small side of a large graph. Every node
    // carries a name, which is what the cuts below key on.
    void buildGraph(size_t nodeCount, size_t rareCount) {
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelID pointLabel = metadata.getOrCreateLabel("Point");
        const LabelID rareLabel = metadata.getOrCreateLabel("Rare");
        const PropertyTypeID nameID = metadata.getOrCreatePropertyType("name", ValueType::String)._id;

        const LabelSet pointLabelSet = LabelSet::fromList({pointLabel});
        const LabelSet rareLabelSet = LabelSet::fromList({pointLabel, rareLabel});

        _names.clear();
        _names.reserve(nodeCount);
        for (size_t node = 0; node < nodeCount; node++) {
            const bool isRare = node < rareCount;
            const NodeID nodeID = builder.addNode(isRare ? rareLabelSet : pointLabelSet);

            _names.push_back("point" + std::to_string(node));
            builder.addNodeProperty<types::String>(nodeID, nameID, _names.back());
        }

        const auto submitResult = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitResult);
    }

    // The db program the pipeline leaves for a query on this graph, as text.
    void generate(std::string_view query, const mlir::db::DBPassContext& context, std::string& program) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        CypherAST ast(nullptr, query);
        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.setV3();
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

        program.clear();
        llvm::raw_string_ostream stream(program);
        module.print(stream);
    }

    // Whether the cut of @param query is left fused on a graph the cost model reads.
    bool fuses(std::string_view query) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();
        const mlir::db::DBPassContext context {&view, false, true};

        std::string program;
        generate(query, context, program);

        return program.find("db.hash_join") != std::string::npos;
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    std::vector<std::string> _names;
};

// 18 nodes, so the product is 324 pairs: cheaper read whole than through a build side of
// 18 indexed keys.
TEST_F(HashJoinCostModelTest, keepsTheProductWhenItIsSmall) {
    buildGraph(18, 0);

    EXPECT_FALSE(fuses("MATCH (n), (m) WHERE n.name = m.name RETURN n, m"));
}

// 400 nodes are 160000 pairs, past the point where indexing one side pays for itself.
TEST_F(HashJoinCostModelTest, fusesWhenTheProductIsLarge) {
    buildGraph(400, 0);

    EXPECT_TRUE(fuses("MATCH (n), (m) WHERE n.name = m.name RETURN n, m"));
}

// The same product under a limit of one row: the product stops as soon as it is met, where
// the join would read all 400 build rows first.
TEST_F(HashJoinCostModelTest, keepsTheProductUnderALimitTheJoinCannotRepay) {
    buildGraph(400, 0);

    EXPECT_FALSE(fuses("MATCH (n), (m) WHERE n.name = m.name RETURN n, m LIMIT 1"));
}

// A limit the product cannot meet within a chunk of the build side leaves the join in
// place: 400 rows against a budget of 3 is not the early exit a smaller side would be.
TEST_F(HashJoinCostModelTest, fusesUnderALimitTooLargeToCutTheProductShort) {
    buildGraph(400, 0);

    EXPECT_TRUE(fuses("MATCH (n), (m) WHERE n.name = m.name RETURN n, m LIMIT 3"));
}

// The labels name ten of the four hundred nodes, so the two sides are the ten - a hundred
// pairs, whatever the size of the graph they sit in.
TEST_F(HashJoinCostModelTest, keepsTheProductWhenTheLabelsNarrowBothSides) {
    buildGraph(400, 10);

    EXPECT_TRUE(fuses("MATCH (n), (m) WHERE n.name = m.name RETURN n, m"));
    EXPECT_FALSE(fuses("MATCH (n:Rare), (m:Rare) WHERE n.name = m.name RETURN n, m"));
}

// The two overrides: forcing takes every cut the pass matches whatever the estimate says,
// and clearing use takes none.
TEST_F(HashJoinCostModelTest, forcingFusesTheSmallestProduct) {
    buildGraph(18, 0);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const mlir::db::DBPassContext forced {&view, true, true};

    std::string program;
    generate("MATCH (n), (m) WHERE n.name = m.name RETURN n, m", forced, program);

    EXPECT_NE(program.find("db.hash_join"), std::string::npos) << program;
}

TEST_F(HashJoinCostModelTest, clearingUseFusesTheLargestProduct) {
    buildGraph(400, 0);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const mlir::db::DBPassContext disabled {&view, false, false};

    std::string program;
    generate("MATCH (n), (m) WHERE n.name = m.name RETURN n, m", disabled, program);

    EXPECT_EQ(program.find("db.hash_join"), std::string::npos) << program;
    EXPECT_NE(program.find("db.cross_product"), std::string::npos) << program;
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
