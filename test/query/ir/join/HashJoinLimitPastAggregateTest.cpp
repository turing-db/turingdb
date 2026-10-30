#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

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

// A limit only cuts the product short when it stands over the product's rows. One past an
// aggregate counts groups, and the aggregate reads every pair first, so a product large
// enough to fuse is as dear under it as under no limit at all.
class HashJoinLimitPastAggregateTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void buildGraph(size_t nodeCount) {
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelID pointLabel = metadata.getOrCreateLabel("Point");
        const PropertyTypeID nameID = metadata.getOrCreatePropertyType("name", ValueType::String)._id;
        const PropertyTypeID keyID = metadata.getOrCreatePropertyType("k", ValueType::Int64)._id;

        const LabelSet pointLabelSet = LabelSet::fromList({pointLabel});

        _names.clear();
        _names.reserve(nodeCount);

        for (size_t node = 0; node < nodeCount; node++) {
            const NodeID nodeID = builder.addNode(pointLabelSet);

            _names.push_back("point" + std::to_string(node));
            builder.addNodeProperty<types::String>(nodeID, nameID, _names.back());
            builder.addNodeProperty<types::Int64>(nodeID, keyID, static_cast<int64_t>(node % 50));
        }

        const auto submitResult = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitResult);
    }

    void generate(std::string_view query, const mlir::db::DBPassContext& context, std::string& program) {
        CypherAST ast(nullptr, query);
        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, *context._view);
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

TEST_F(HashJoinLimitPastAggregateTest, fusesUnderALimitOverACount) {
    buildGraph(400);

    EXPECT_TRUE(fuses("MATCH (n), (m) WHERE n.name = m.name RETURN count(*)"));
    EXPECT_TRUE(fuses("MATCH (n), (m) WHERE n.name = m.name RETURN count(*) LIMIT 1"));
}

TEST_F(HashJoinLimitPastAggregateTest, fusesUnderALimitOverSortedGroups) {
    buildGraph(1000);

    EXPECT_TRUE(fuses("MATCH (n), (m) WHERE n.k = m.k RETURN m.k, count(*) ORDER BY m.k"));
    EXPECT_TRUE(fuses("MATCH (n), (m) WHERE n.k = m.k RETURN m.k, count(*) ORDER BY m.k LIMIT 3"));
}

TEST_F(HashJoinLimitPastAggregateTest, keepsTheProductUnderALimitOverItsRows) {
    buildGraph(400);

    EXPECT_FALSE(fuses("MATCH (n), (m) WHERE n.name = m.name RETURN n.name, m.name LIMIT 1"));
    EXPECT_FALSE(fuses("MATCH (n), (m) WHERE n.name = m.name WITH n, m LIMIT 1 RETURN count(*)"));
}

TEST_F(HashJoinLimitPastAggregateTest, keepsTheProductUnderALimitOverItsKeys) {
    buildGraph(1000);

    EXPECT_FALSE(fuses("MATCH (n), (m) WHERE n.k = m.k RETURN m.k LIMIT 3"));
    EXPECT_FALSE(fuses("MATCH (n), (m) WHERE n.k = m.k WITH m LIMIT 3 RETURN m.k, count(*) ORDER BY m.k"));
}
