#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A scan of edges makes one row per edge, so the node at each end of it repeats once for every
// edge it has there, as the node a hop walks from does. A value read off it whose every use
// follows a filter is read for the rows the filter keeps, as it is behind a hop. Remy and Adam
// are the only people with an age, 32; the edges lasting more than 15 leave Remy three times,
// and Adam, Ghosts and Luc once each.
class PlaceEdgeScanReadsTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    void generateProgram(std::string_view query, mlir::OwningOpRef<mlir::ModuleOp>& module, std::string& program) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();
        Graph* graph = system.getGraph(_graphName);

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::OpBuilder builder(&_context);
        module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);

        llvm::raw_string_ostream stream(program);
        moduleOp.print(stream);
    }

    // The db program of @param query scans its edges with a @param ScanOp, and reads
    // @param property @param total times, @param before of them in front of the filter
    template <typename ScanOp>
    void expectReadsBeforeTheFilter(std::string_view query, llvm::StringRef property, size_t before, size_t total) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        std::string program;
        ASSERT_NO_FATAL_FAILURE(generateProgram(query, module, program));

        size_t scans = 0;
        size_t placed = 0;
        size_t reads = 0;
        bool filterSeen = false;
        module->walk<mlir::WalkOrder::PreOrder>([property, &scans, &placed, &reads, &filterSeen](mlir::Operation* op) {
            filterSeen = filterSeen || mlir::isa<mlir::db::FilterOp>(op);
            if (mlir::isa<ScanOp>(op)) {
                scans++;
            }

            mlir::db::GetNodeProperties read = mlir::dyn_cast<mlir::db::GetNodeProperties>(op);
            if (!read || read.getProperty() != property) {
                return;
            }

            reads++;
            if (!filterSeen) {
                placed++;
            }
        });

        EXPECT_EQ(scans, 1u) << query << "\n" << program;
        EXPECT_TRUE(filterSeen) << query << "\n" << program;
        EXPECT_EQ(placed, before) << query << "\n" << program;
        EXPECT_EQ(reads, total) << query << "\n" << program;
    }

private:
    mlir::MLIRContext _context;
};

TEST_F(PlaceEdgeScanReadsTest, readsTheSourceOfAnEdgeScanAfterTheFilter) {
    const std::string_view query = "MATCH (n)-[e]->(m) WITH n.name AS nm, e WHERE e.duration > 15 RETURN nm";
    expectReadsBeforeTheFilter<mlir::db::ScanEdges>(query, "name", 0, 1);
    expectRows(query, {{"Remy"}, {"Remy"}, {"Remy"}, {"Adam"}, {"Ghosts"}, {"Luc"}});
}

TEST_F(PlaceEdgeScanReadsTest, readsTheSourceOfAnEdgeScanByTypeAfterTheFilter) {
    const std::string_view query = "MATCH (n)-[e:KNOWS_WELL]->(m) WITH n.name AS nm, e WHERE e.duration > 15 RETURN nm";
    expectReadsBeforeTheFilter<mlir::db::ScanEdgesByType>(query, "name", 0, 1);
    expectRows(query, {{"Remy"}, {"Adam"}, {"Ghosts"}});
}

TEST_F(PlaceEdgeScanReadsTest, readsTheLabelledSourceOfAnOutEdgeScanAfterTheFilter) {
    const std::string_view query = "MATCH (n:Person)-->(m) WITH n.name AS nm, m WHERE m.age > 30 RETURN nm";
    expectReadsBeforeTheFilter<mlir::db::ScanOutEdgesByLabelSrc>(query, "name", 0, 1);
    expectRows(query, {{"Remy"}, {"Adam"}});
}

TEST_F(PlaceEdgeScanReadsTest, readsTheTargetOfAnOutEdgeScanAfterTheFilter) {
    const std::string_view query = "MATCH (n:Person)-[e]->(m) WITH m.name AS mn, e WHERE e.duration > 15 RETURN mn";
    expectReadsBeforeTheFilter<mlir::db::ScanOutEdgesByLabelSrc>(query, "name", 0, 1);
    expectRows(query, {{"Adam"}, {"Ghosts"}, {"Eighties"}, {"Remy"}, {"Animals"}});
}

TEST_F(PlaceEdgeScanReadsTest, readsTheLabelledTargetOfAnInEdgeScanAfterTheFilter) {
    const std::string_view query = "MATCH (n:Person)<--(m) WITH n.name AS nm, m WHERE m.age > 30 RETURN nm";
    expectReadsBeforeTheFilter<mlir::db::ScanInEdgesByLabelTgt>(query, "name", 0, 1);
    expectRows(query, {{"Remy"}, {"Adam"}});
}

// The filter reads the name of the interest, and only the projection the name of the person
TEST_F(PlaceEdgeScanReadsTest, readsAnEndOfAnEdgeScanLabelledAtBothEndsAfterTheFilter) {
    const std::string_view query = "MATCH (n:Person)<--(m:Interest) WITH n.name AS nm, m WHERE m.name > 'C' RETURN nm";
    expectReadsBeforeTheFilter<mlir::db::ScanInEdgesByLabelSrcTgt>(query, "name", 1, 2);
    expectRows(query, {{"Remy"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
