#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A scan or a hop outputs the types of the edges it outputs, so a type test over those edges
// reads that column, carried to it the way an earlier read of the types would be. The types
// are read only of edges nothing output them beside.
class EdgeTypeColumnReadsTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

protected:
    mlir::OwningOpRef<mlir::ModuleOp> generate(std::string_view query) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::OpBuilder builder(&_context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        return owningModule;
    }

    // The edges come from an op named @param producer, and @param expectedInHops of the
    // @param expected type reads stand in a hop region
    void expectTypeReads(std::string_view query, llvm::StringRef producer, size_t expected, size_t expectedInHops) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = generate(query);
        ASSERT_TRUE(module) << query;

        mlir::ModuleOp program = *module;

        size_t count = 0;
        size_t countInHops = 0;
        size_t producers = 0;
        program.walk([&count, &countInHops, &producers, producer](mlir::Operation* op) {
            if (op->getName().getStringRef() == producer) {
                producers++;
            }

            if (!mlir::isa<mlir::db::GetEdgeTypes>(op)) {
                return;
            }

            count++;
            if (op->getParentOfType<mlir::db::ExplorePaths>()) {
                countInHops++;
            }
        });

        std::string text;
        llvm::raw_string_ostream stream(text);
        program.print(stream);

        EXPECT_TRUE(mlir::succeeded(mlir::verify(program))) << query << "\n" << text;
        EXPECT_GT(producers, 0u) << query << "\n" << text;
        EXPECT_EQ(count, expected) << query << "\n" << text;
        EXPECT_EQ(countInHops, expectedInHops) << query << "\n" << text;
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(EdgeTypeColumnReadsTest, readsTheScannedTypesAfterAWith) {
    expectTypeReads("MATCH ()-[e]->() WITH e RETURN e.name, e:KNOWS_WELL", "db.scan_edges", 0, 0);
}

TEST_F(EdgeTypeColumnReadsTest, fusesATypeFilterAfterAWithIntoTheByTypeRead) {
    expectTypeReads("MATCH (n)-[e]->(m) WITH e, n WHERE e:KNOWS_WELL RETURN n", "db.scan_edges_by_type", 0, 0);
    expectTypeReads("MATCH (a)-->(n)-[e]->(m) WITH e, m WHERE e:KNOWS_WELL RETURN m", "db.get_out_edges_by_type", 0, 0);
}

TEST_F(EdgeTypeColumnReadsTest, readsTheScannedTypesAcrossAFilter) {
    expectTypeReads("MATCH ()-[e]->() WITH e WHERE e.duration > 15 RETURN e.name, e:KNOWS_WELL", "db.scan_edges", 0, 0);
}

TEST_F(EdgeTypeColumnReadsTest, readsTheScannedTypesAcrossAnOrderByAndALimit) {
    expectTypeReads("MATCH ()-[e]->() WITH e ORDER BY e.name LIMIT 3 RETURN e.name, e:KNOWS_WELL", "db.scan_edges", 0, 0);
}

TEST_F(EdgeTypeColumnReadsTest, readsTheScannedTypesIntoACaseBranch) {
    expectTypeReads("MATCH ()-[e]->() WITH e RETURN e.name, CASE WHEN e.duration > 15 THEN e:KNOWS_WELL END",
                    "db.scan_edges",
                    0,
                    0);
}

TEST_F(EdgeTypeColumnReadsTest, readsTheScannedTypesIntoAHopRegion) {
    expectTypeReads("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL OR r.duration > 15 "
                    "MATCH (n)(()-[e]->() WHERE r:KNOWS_WELL AND e.duration > 15){1}(m) RETURN a, n, m",
                    "db.scan_out_edges_by_label_src",
                    0,
                    0);
    expectTypeReads("MATCH (a:Person)-[r]->(n) "
                    "MATCH (n)(()-[e]->() WHERE r:KNOWS_WELL AND e.duration > 15){1,2}(m) RETURN a, m",
                    "db.scan_out_edges_by_label_src",
                    0,
                    0);
}

TEST_F(EdgeTypeColumnReadsTest, readsTheTypesEveryEdgeScanOutputs) {
    expectTypeReads("MATCH ()-[e]->() WITH e RETURN e:KNOWS_WELL", "db.scan_edges", 0, 0);
    expectTypeReads("MATCH ()-[e:KNOWS_WELL|INTERESTED_IN]->() WITH e RETURN e:KNOWS_WELL", "db.scan_edges_by_type", 0, 0);
    expectTypeReads("MATCH (a:Person)-[e]->(b) WITH a, e RETURN a, e:KNOWS_WELL", "db.scan_out_edges_by_label_src", 0, 0);
    expectTypeReads("MATCH (a:Person)<-[e]-(b) WITH a, e RETURN a, e:KNOWS_WELL", "db.scan_in_edges_by_label_tgt", 0, 0);
    expectTypeReads("MATCH (a)-[e]->(b:Person) WITH b, e RETURN b, e:KNOWS_WELL", "db.scan_out_edges_by_label_tgt", 0, 0);
    expectTypeReads("MATCH (a)<-[e]-(b:Person) WITH b, e RETURN b, e:KNOWS_WELL", "db.scan_in_edges_by_label_src", 0, 0);
    expectTypeReads("MATCH (a:Person)-[e]->(b:Interest) WITH a, e RETURN a, e:KNOWS_WELL",
                    "db.scan_out_edges_by_label_src_tgt",
                    0,
                    0);
    expectTypeReads("MATCH (a:Interest)<-[e]-(b:Person) WITH a, e RETURN a, e:KNOWS_WELL",
                    "db.scan_in_edges_by_label_src_tgt",
                    0,
                    0);
}

TEST_F(EdgeTypeColumnReadsTest, readsTheTypesEveryHopOutputs) {
    expectTypeReads("MATCH (a)-[e]->(b) WHERE a.name = 'Remy' WITH b, e RETURN b, e:KNOWS_WELL", "db.get_out_edges", 0, 0);
    expectTypeReads("MATCH (a)<-[e]-(b) WHERE a.name = 'Remy' WITH b, e RETURN b, e:KNOWS_WELL", "db.get_in_edges", 0, 0);
    expectTypeReads("MATCH (a)-[e]-(b) WHERE a.name = 'Remy' WITH b, e RETURN b, e:KNOWS_WELL", "db.get_edges", 0, 0);
    expectTypeReads("MATCH (a)-[e:KNOWS_WELL|INTERESTED_IN]->(b) WHERE a.name = 'Remy' WITH b, e RETURN b, e:KNOWS_WELL",
                    "db.get_out_edges_by_type",
                    0,
                    0);
    expectTypeReads("MATCH (a)<-[e:KNOWS_WELL|INTERESTED_IN]-(b) WHERE a.name = 'Remy' WITH b, e RETURN b, e:KNOWS_WELL",
                    "db.get_in_edges_by_type",
                    0,
                    0);
    expectTypeReads("MATCH (a)-[e]->(b:Person) WHERE a.name = 'Remy' WITH b, e RETURN b, e:KNOWS_WELL",
                    "db.get_out_edges_by_label",
                    0,
                    0);
    expectTypeReads("MATCH (a)<-[e]-(b:Person) WHERE a.name = 'Remy' WITH b, e RETURN b, e:KNOWS_WELL",
                    "db.get_in_edges_by_label",
                    0,
                    0);
    expectTypeReads("MATCH (a)-[e:KNOWS_WELL|INTERESTED_IN]->(b:Person) WHERE a.name = 'Remy' WITH b, e RETURN b, e:KNOWS_WELL",
                    "db.get_out_edges_by_type_and_label",
                    0,
                    0);
    expectTypeReads("MATCH (a)<-[e:KNOWS_WELL|INTERESTED_IN]-(b:Person) WHERE a.name = 'Remy' WITH b, e RETURN b, e:KNOWS_WELL",
                    "db.get_in_edges_by_type_and_label",
                    0,
                    0);
}

TEST_F(EdgeTypeColumnReadsTest, readsTheTypesOfAHopCarriedThroughAnotherHop) {
    expectTypeReads("MATCH (a)-[e]->(b)-->(c) WITH e, c RETURN e:KNOWS_WELL, c", "db.scan_edges", 0, 0);
}

TEST_F(EdgeTypeColumnReadsTest, readsTheTypesOfEdgesNothingOutputThemBeside) {
    expectTypeReads("MATCH (n)(()-[e]->() WHERE e:KNOWS_WELL OR e.duration > 15){1,2}(m) RETURN m",
                    "db.explore_paths",
                    1,
                    1);
    expectTypeReads("MATCH p = (a)-[*1..2]->(b) WHERE a.name = 'Remy' UNWIND relationships(p) AS e RETURN e:KNOWS_WELL",
                    "db.explore_paths",
                    1,
                    0);
}
