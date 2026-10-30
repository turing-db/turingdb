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
    // Rare as well, so a query naming Rare scans a small side of a large graph, and
    // @param edgesPerNode LINKS edges out of each. Every node carries a name, which is
    // what the cuts below key on.
    void buildGraph(size_t nodeCount, size_t rareCount, size_t edgesPerNode = 0) {
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelID pointLabel = metadata.getOrCreateLabel("Point");
        const LabelID rareLabel = metadata.getOrCreateLabel("Rare");
        const EdgeTypeID linksType = metadata.getOrCreateEdgeType("LINKS");
        const PropertyTypeID nameID = metadata.getOrCreatePropertyType("name", ValueType::String)._id;

        const LabelSet pointLabelSet = LabelSet::fromList({pointLabel});
        const LabelSet rareLabelSet = LabelSet::fromList({pointLabel, rareLabel});

        _names.clear();
        _names.reserve(nodeCount);

        std::vector<NodeID> nodes;
        nodes.reserve(nodeCount);
        for (size_t node = 0; node < nodeCount; node++) {
            const bool isRare = node < rareCount;
            nodes.push_back(builder.addNode(isRare ? rareLabelSet : pointLabelSet));

            _names.push_back("point" + std::to_string(node));
            builder.addNodeProperty<types::String>(nodes.back(), nameID, _names.back());
        }

        for (size_t node = 0; node < nodeCount; node++) {
            for (size_t edge = 0; edge < edgesPerNode; edge++) {
                builder.addEdge(linksType, nodes[node], nodes[(node + edge + 1) % nodeCount]);
            }
        }

        const auto submitResult = change->access().submit(*_jobSystem);
        ASSERT_TRUE(submitResult);
    }

    // A hundred Point nodes, the first ten Rare as well, with three LINKS edges out of each,
    // a RARE_LINK edge out of Rare nodes 0 and 1 into node 50, and thirty FANS edges out of
    // node 50 alone, so a walk reaching node 50 by RARE_LINK has every FANS edge to follow.
    void buildGraphWithRareLinks() {
        buildGraph(100, 10, 3);

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const EdgeTypeID rareLinkType = metadata.getOrCreateEdgeType("RARE_LINK");
        const EdgeTypeID fansType = metadata.getOrCreateEdgeType("FANS");
        const NodeID hub {50};

        builder.addEdge(rareLinkType, NodeID {0}, hub);
        builder.addEdge(rareLinkType, NodeID {1}, hub);
        for (size_t fan = 60; fan < 90; fan++) {
            builder.addEdge(fansType, hub, NodeID {fan});
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

// Which side the join buffers, read off the db program: the built side is its right
// factor, so a scan standing after the join's own `} factor {` is the side being built.
TEST_F(HashJoinCostModelTest, buildsTheSideTheLabelsNarrow) {
    buildGraph(400, 10);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const mlir::db::DBPassContext forced {&view, true, true};

    // Ten Rare nodes against four hundred: whichever side the query writes it on, the ten
    // are what the join holds and the four hundred what it streams.
    std::string program;
    generate("MATCH (n:Rare), (m) WHERE n.name = m.name RETURN n, m", forced, program);
    EXPECT_LT(program.find("} factor {"), program.find("db.scan_nodes_by_label")) << program;

    generate("MATCH (n), (m:Rare) WHERE n.name = m.name RETURN n, m", forced, program);
    EXPECT_LT(program.find("} factor {"), program.find("db.scan_nodes_by_label")) << program;
}

// A property equality keeps a tenth of the nodes it reads, the System R default, so the
// four hundred named nodes filtered to one name are forty rows against the four hundred
// of the bare scan, and the filtered side is the one to hold.
TEST_F(HashJoinCostModelTest, buildsTheSideItsFilterNarrows) {
    buildGraph(400, 0);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const mlir::db::DBPassContext forced {&view, true, true};

    std::string program;
    generate("MATCH (n {name: 'point3'}), (m) WHERE n.name = m.name RETURN n, m", forced, program);

    EXPECT_LT(program.find("} factor {"), program.find("db.scan_nodes_by_property_value")) << program;
}

// A factor a hop seeds carries a row per edge, not per node, and the graph counts those
// too: a hundred nodes with three edges each is three hundred rows against the hundred the
// bare scan beside it makes, so the bare scan is the side to hold.
TEST_F(HashJoinCostModelTest, buildsTheScanRatherThanTheEdgesItIsCrossedWith) {
    buildGraph(100, 0, 3);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    ASSERT_EQ(reader.getEdgeCount(), 300u);

    const mlir::db::DBPassContext forced {&view, true, true};

    std::string program;
    generate("MATCH (a)-->(b), (m) WHERE b.name = m.name RETURN a, m", forced, program);

    EXPECT_LT(program.find("db.scan_edges"), program.find("} factor {")) << program;
}

// A hop out of a labelled scan carries its fan-out, whether it still reads as a hop or as
// the by-label edge scan it fuses into: ten Rare nodes of average degree three make thirty
// rows, so the bare scan of the same ten is the smaller side and the one to hold.
TEST_F(HashJoinCostModelTest, sizesAHopByItsFanOut) {
    buildGraph(100, 10, 3);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const mlir::db::DBPassContext forced {&view, true, true};

    std::string program;
    generate("MATCH (m:Rare), (n:Rare)-->(x) WHERE m.name = x.name RETURN m, x", forced, program);

    // The hop is the side that streams, so it stands in the join's first factor.
    EXPECT_LT(program.find("db.scan_out_edges_by_label_src"), program.find("} factor {")) << program;
}

// An unwind of a literal list makes a row per element, and the elements are in the IR, so
// the side it seeds is sized through it: twenty of them over the ten Rare nodes they are
// crossed with are two hundred rows, against the hundred of the plain scan beside them -
// so the scan is what the join holds and the product what it streams.
TEST_F(HashJoinCostModelTest, sizesAFactorThroughTheListItUnwinds) {
    buildGraph(100, 10, 0);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const mlir::db::DBPassContext forced {&view, true, true};

    std::string program;
    generate("MATCH (n) UNWIND [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20] AS x "
             "MATCH (m:Rare) WHERE n.name = m.name RETURN n, m, x",
             forced,
             program);

    // The product stands in the join's first factor, the side that streams; the scan it
    // was crossed with would stand there instead were the list left uncounted.
    EXPECT_LT(program.find("db.cross_product"), program.find("} factor {")) << program;
}

// A join makes as many rows as its smaller side, so the first MATCH joins the 20 pairs to
// the 400 nodes in 20 rows. Read as the product its equality keeps a tenth of, they would
// be 800, twice b's scan: the second join holds the 20 rows and streams the scan.
TEST_F(HashJoinCostModelTest, sizesAJoinByItsSmallerSide) {
    buildGraph(400, 0);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const mlir::db::DBPassContext forced {&view, true, true};

    std::string pairs;
    for (size_t pair = 0; pair < 20; pair++) {
        pairs += pair == 0 ? "[" : ", [";
        pairs += "'" + _names[2 * pair] + "', '" + _names[2 * pair + 1] + "']";
    }

    std::string program;
    generate("UNWIND [" + pairs + "] AS p "
             "MATCH (a:Point {name: p[0]}) MATCH (b:Point {name: p[1]}) RETURN a, b",
             forced,
             program);

    const size_t outerJoin = program.find("db.hash_join");
    ASSERT_NE(outerJoin, std::string::npos) << program;

    const size_t innerJoin = program.find("db.hash_join", outerJoin + 1);
    ASSERT_NE(innerJoin, std::string::npos) << program;
    EXPECT_LT(program.find("} factor {"), innerJoin) << program;
}

// Each MATCH crosses the unwound rows with the 400 nodes and keeps those whose name is a
// key the row reads off its pair, so each is a cut the join takes, and no OR of the keys
// is evaluated per node in its place.
TEST_F(HashJoinCostModelTest, fusesTheKeyLookupsOfUnwoundRows) {
    buildGraph(400, 0);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const mlir::db::DBPassContext context {&view, false, true};

    std::string program;
    generate("UNWIND [['point1', 'point2'], ['point3', 'point4']] AS p "
             "MATCH (a:Point {name: p[0]}) MATCH (b:Point {name: p[1]}) RETURN a, b",
             context,
             program);

    const size_t firstJoin = program.find("db.hash_join");
    ASSERT_NE(firstJoin, std::string::npos) << program;
    EXPECT_NE(program.find("db.hash_join", firstJoin + 1), std::string::npos) << program;
    EXPECT_EQ(program.find("db.cross_product"), std::string::npos) << program;

    generate("UNWIND ['point1', 'point2', 'point3'] AS x MATCH (a:Point {name: x}) RETURN a",
             context,
             program);

    EXPECT_NE(program.find("db.hash_join"), std::string::npos) << program;
    EXPECT_EQ(program.find("db.or"), std::string::npos) << program;
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

// An edge scan by type is read at the edges of its type: the two RARE_LINK edges against the
// hundred nodes of the bare scan, so the edges are the side to hold, where every edge of the
// graph would have been 308.
TEST_F(HashJoinCostModelTest, sizesAnEdgeScanByItsType) {
    buildGraphWithRareLinks();

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const mlir::db::DBPassContext forced {&view, true, true};

    std::string program;
    generate("MATCH (a)-[:RARE_LINK]->(b), (m) WHERE b.name = m.name RETURN a, m", forced, program);

    EXPECT_LT(program.find("} factor {"), program.find("RARE_LINK")) << program;
}

// A typed hop out of the ten Rare nodes makes the two edges of its type, not the 3.2 edges
// of every type each Rare node has, so it is the side to hold against the ten.
TEST_F(HashJoinCostModelTest, sizesAHopByTheEdgesOfItsType) {
    buildGraphWithRareLinks();

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const mlir::db::DBPassContext forced {&view, true, true};

    std::string program;
    generate("MATCH (m:Rare), (n:Rare)-[:RARE_LINK]->(x) WHERE m.name = x.name RETURN m, x", forced, program);

    EXPECT_LT(program.find("} factor {"), program.find("RARE_LINK")) << program;
}

// Both RARE_LINK edges reach node 50, which has all thirty FANS edges, so the two hops make
// sixty rows rather than the 0.6 the average FANS out-degree of the hundred nodes gives, and
// the ten Rare nodes are the side to hold.
TEST_F(HashJoinCostModelTest, sizesASecondHopByTheNodesTheFirstReaches) {
    buildGraphWithRareLinks();

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const mlir::db::DBPassContext forced {&view, true, true};

    std::string program;
    generate("MATCH (m:Rare), (n:Rare)-[:RARE_LINK]->(h)-[:FANS]->(x) WHERE m.name = x.name RETURN m, x", forced, program);

    EXPECT_LT(program.find("RARE_LINK"), program.find("} factor {")) << program;
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
