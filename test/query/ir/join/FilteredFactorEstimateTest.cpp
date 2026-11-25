#include <gtest/gtest.h>

#include <stdint.h>

#include <string>
#include <string_view>
#include <vector>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "Graph.h"
#include "ID.h"
#include "JobSystem.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

#include "DBPasses.h"

#include "HashJoinQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr int64_t PATHWAY_COUNT = 400;
constexpr int64_t HUMAN_COUNT = 200;
constexpr int64_t MOUSE_COUNT = 10;
constexpr int64_t EVENTS_PER_PATHWAY = 4;
constexpr int64_t EVENT_NAME_COUNT = 100;
constexpr int64_t OTHER_COUNT = 20000;
constexpr int64_t TINY_COUNT = 3;

// MATCH (t:Tiny), (q:Pathway)-[:hasEvent]->(s:ReactionLikeEvent)
// WHERE q.speciesName = 'Mus musculus' AND t.displayName = s.displayName RETURN count(*),
// with the filters codegen writes still between the scan, the hop and the label check
const char* const tinyAgainstMouseEvents = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %t = db.scan_nodes_by_label(["Tiny"]) : !db.column<!storage.node_id>
    %tName = db.get_node_properties(%t, "displayName") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    db.yield %tName : !db.column<!storage.nullable<!storage.string>>
  } factor {
    %q = db.scan_nodes_by_label(["Pathway"]) : !db.column<!storage.node_id>
    %species = db.get_node_properties(%q, "speciesName") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    %mouse = db.constant("Mus musculus" : !storage.string)
    %isMouse = db.eq %species, %mouse : (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.string>) -> !db.column<!storage.bool>
    %mice = db.filter(%isMouse, {%q}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    %from, %edges, %types, %s = db.get_out_edges(%mice, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %isEvent = db.check_edge_type_constraint(%types, ["hasEvent"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
    %reached = db.filter(%isEvent, {%s}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    %labels = db.get_node_label_set(%reached) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %isReaction = db.check_label_constraint(%labels, ["ReactionLikeEvent"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    %events = db.filter(%isReaction, {%reached}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    %sName = db.get_node_properties(%events, "displayName") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    db.yield %sName : !db.column<!storage.nullable<!storage.string>>
  }
  %eq = db.eq %p#0, %p#1 : (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>) -> !db.column<!storage.bool>
  %tName, %sName = db.filter(%eq, {%p#0, %p#1}) : (!db.column<!storage.bool>, !db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>) -> (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>)
  %count = db.count(%tName) rows : (!db.column<!storage.nullable<!storage.string>>) -> !db.column<none>
  db.output(%count) names ["count(*)"] : !db.column<none>
  return
}
)mlir";

}

// Pathway is 400 of 22,000 nodes. The 10 Mus musculus pathways reach 40 events, which the
// estimate of their factor read as 0.01 rows: every filter made a new column, so the hop
// after the species filter and the label check after the hop forgot the Pathway label.
class FilteredFactorEstimateTest : public HashJoinQueryTest {
protected:
    // SimpleGraph, then 400 Pathway nodes {speciesName, pid}: 200 Homo sapiens, 10 Mus
    // musculus, 190 Other species. Each has a hasEvent edge to 4 ReactionLikeEvent nodes
    // named n0 to n99 in turn, so the 800 human events hold each name 8 times and the 40
    // mouse events n0 to n39 once. 3 Tiny nodes are named n0 to n2, and 20,000 Other nodes
    // make Pathway a small share of the graph.
    void initialize() override {
        HashJoinQueryTest::initialize();

        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(_graph.get(), &jobSystem);

        for (int64_t pathwayIndex = 0; pathwayIndex < PATHWAY_COUNT; pathwayIndex++) {
            const bool isHuman = pathwayIndex < HUMAN_COUNT;
            const bool isMouse = !isHuman && pathwayIndex < HUMAN_COUNT + MOUSE_COUNT;
            const std::string_view species = isHuman ? "Homo sapiens" : (isMouse ? "Mus musculus" : "Other species");

            const NodeID pathway = writer.addNode({"Pathway"});
            writer.addNodeProperty<types::String>(pathway, "speciesName", std::string_view(species));
            writer.addNodeProperty<types::Int64>(pathway, "pid", int64_t {pathwayIndex});

            for (int64_t eventIndex = 0; eventIndex < EVENTS_PER_PATHWAY; eventIndex++) {
                const int64_t nameIndex = (pathwayIndex * EVENTS_PER_PATHWAY + eventIndex) % EVENT_NAME_COUNT;
                const std::string name = "n" + std::to_string(nameIndex);

                const NodeID event = writer.addNode({"ReactionLikeEvent"});
                writer.addNodeProperty<types::String>(event, "displayName", std::string_view(name));
                writer.addEdge("hasEvent", pathway, event);
            }
        }

        for (int64_t tinyIndex = 0; tinyIndex < TINY_COUNT; tinyIndex++) {
            const std::string name = "n" + std::to_string(tinyIndex);

            const NodeID tiny = writer.addNode({"Tiny"});
            writer.addNodeProperty<types::String>(tiny, "displayName", std::string_view(name));
        }

        for (int64_t otherIndex = 0; otherIndex < OTHER_COUNT; otherIndex++) {
            writer.addNode({"Other"});
        }

        ASSERT_TRUE(writer.submit());
    }

    void plannedProgram(std::string_view query, std::string& program) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
        generate(query, view, module.get(), false);

        render(module.get(), program);
    }

    void expectHashJoin(std::string_view query, size_t expected) {
        std::string program;
        plannedProgram(query, program);

        EXPECT_TRUE(contains(program, "db.hash_join")) << "query: " << query << "\n" << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << "query: " << query << "\n" << program;

        StringRowSink sink;
        runQuery(query, sink, false);

        const Rows expectedRows {{std::to_string(expected)}};
        EXPECT_EQ(sink.getRows(), expectedRows) << "query: " << query;
    }
};

// The mouse factor makes 400 / 200 * 4 = 8 rows by the estimate, more than the 3 Tiny
// nodes, so the join builds Tiny. Read as 0.01 rows, the factor was built instead.
TEST_F(FilteredFactorEstimateTest, buildsTheSideAFilteredHopMakesMoreRowsThan) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(tinyAgainstMouseEvents,
                                                                                         mlir::ParserConfig(&_context));
    ASSERT_TRUE(module);

    const mlir::db::DBPassContext passContext {&view, true, true};
    mlir::PassManager passManager(&_context);
    passManager.addPass(mlir::db::createFuseHashJoin(&passContext));
    ASSERT_TRUE(mlir::succeeded(passManager.run(module.get())));

    std::string program;
    render(module.get(), program);

    const size_t join = program.find("db.hash_join");
    ASSERT_NE(join, std::string::npos) << program;

    const size_t builtBegin = program.find("} factor {", join);
    const size_t builtEnd = program.find("} on", builtBegin);
    ASSERT_NE(builtBegin, std::string::npos) << program;
    ASSERT_NE(builtEnd, std::string::npos) << program;

    const std::string_view built = std::string_view(program).substr(builtBegin, builtEnd - builtBegin);
    EXPECT_TRUE(contains(built, "Tiny")) << program;
    EXPECT_FALSE(contains(built, "get_out_edges")) << program;
}

// Each of the 40 mouse events shares its name with 8 human events
TEST_F(FilteredFactorEstimateTest, joinsTwoFilteredHopsOnAProperty) {
    expectHashJoin("MATCH (p:Pathway)-[:hasEvent]->(r:ReactionLikeEvent), (q:Pathway)-[:hasEvent]->(s:ReactionLikeEvent) "
                   "WHERE r.displayName = s.displayName AND p.speciesName = 'Homo sapiens' AND q.speciesName = 'Mus musculus' "
                   "RETURN count(*)",
                   40 * 8);
}

// The 5 mouse pathways of pid below 205 reach the 20 events n0 to n19
TEST_F(FilteredFactorEstimateTest, joinsTwoFilteredHopsOnAPropertyWithTwoFiltersOnOneSide) {
    expectHashJoin("MATCH (p:Pathway)-[:hasEvent]->(r:ReactionLikeEvent), (q:Pathway)-[:hasEvent]->(s:ReactionLikeEvent) "
                   "WHERE r.displayName = s.displayName AND p.speciesName = 'Homo sapiens' AND q.speciesName = 'Mus musculus' "
                   "AND q.pid < 205 RETURN count(*)",
                   20 * 8);
}
