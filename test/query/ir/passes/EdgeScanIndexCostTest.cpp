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

// Which index a scan labelled on both ends walks. The walk visits every edge the index holds
// under that end's labels, so the end with fewer edges is the cheaper one, whatever its node
// count. Here 2 Hub nodes hold 105 out-edges and 5 Target nodes 5 in-edges.
class EdgeScanIndexCostTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        buildGraph();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // 2 Hub nodes, 100 Leaf nodes and 5 Target nodes. The first hub links to every Leaf and
    // Target node, the second to none, and every Target node links to one Leaf node.
    void buildGraph() {
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelSet hubLabelSet = LabelSet::fromList({metadata.getOrCreateLabel("Hub")});
        const LabelSet leafLabelSet = LabelSet::fromList({metadata.getOrCreateLabel("Leaf")});
        const LabelSet targetLabelSet = LabelSet::fromList({metadata.getOrCreateLabel("Target")});
        const EdgeTypeID linksType = metadata.getOrCreateEdgeType("LINKS");

        const NodeID hub = builder.addNode(hubLabelSet);
        builder.addNode(hubLabelSet);

        std::vector<NodeID> leaves;
        for (size_t leaf = 0; leaf < 100; leaf++) {
            leaves.push_back(builder.addNode(leafLabelSet));
            builder.addEdge(linksType, hub, leaves.back());
        }

        for (size_t target = 0; target < 5; target++) {
            const NodeID targetNode = builder.addNode(targetLabelSet);
            builder.addEdge(linksType, hub, targetNode);
            builder.addEdge(linksType, targetNode, leaves[target]);
        }

        const auto submitResult = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitResult);
    }

    // The db program the pipeline leaves for a query on this graph, as text.
    void generate(std::string_view query, std::string& program) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();
        const mlir::db::DBPassContext context {._view = &view};

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

        program.clear();
        llvm::raw_string_ostream stream(program);
        module.print(stream);
    }

    void expectScan(std::string_view query, std::string_view scan) {
        std::string program;
        generate(query, program);

        EXPECT_NE(program.find(scan), std::string::npos) << program;
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(EdgeScanIndexCostTest, outHopWalksTheTargetIndexWhenItHoldsFewerEdges) {
    expectScan("MATCH (h:Hub)-->(t:Target) RETURN h, t",
               "db.scan_in_edges_by_label_src_tgt([\"Hub\"], [\"Target\"])");
}

TEST_F(EdgeScanIndexCostTest, inHopWalksTheTargetIndexWhenItHoldsFewerEdges) {
    expectScan("MATCH (t:Target)<--(h:Hub) RETURN h, t",
               "db.scan_in_edges_by_label_src_tgt([\"Hub\"], [\"Target\"])");
}

// 5 Target out-edges against 105 Leaf in-edges: the source index, which node counts pick too.
TEST_F(EdgeScanIndexCostTest, outHopWalksTheSourceIndexWhenItHoldsFewerEdges) {
    expectScan("MATCH (t:Target)-->(l:Leaf) RETURN t, l",
               "db.scan_out_edges_by_label_src_tgt([\"Target\"], [\"Leaf\"])");
}

// 5 Target out-edges against 5 Target in-edges: a tie reads the source's index.
TEST_F(EdgeScanIndexCostTest, tieWalksTheSourceIndex) {
    expectScan("MATCH (a:Target)<--(b:Target) RETURN a, b",
               "db.scan_out_edges_by_label_src_tgt([\"Target\"], [\"Target\"])");
}
