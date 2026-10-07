#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

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

#include "IRTestOps.h"

using namespace db;
using namespace turing::test;


// Generates the db program a Cypher query compiles to, passes included, so a test reads
// the shape the engine will lower rather than a hand-written approximation of it.
class RemoveRedundantLabelChecksCodegenTest : public TuringTest {
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
        generator.generate(&ast, ast.queries().front());

        return owningModule;
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

// e is checked where the first input hop reaches it, where the second reaches it again, and
// once more after the equality that rebinds the two; the hops take the first two.
TEST_F(RemoveRedundantLabelChecksCodegenTest, reboundEndpointKeepsNoLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a:Person)-[:KNOWS_WELL]->(r1:Person), "
                                                              "(a)-[:KNOWS_WELL]->(r2:Person), "
                                                              "(r1)-[:INTERESTED_IN]->(e:Interest), "
                                                              "(r2)-[:INTERESTED_IN]->(e) "
                                                              "RETURN count(e)");

    EXPECT_EQ(countOps<mlir::db::GetOutEdgesByTypeAndLabel>(*module), 4u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

// b is only a Person where the pattern says so, so the check on the second hop's target stays.
TEST_F(RemoveRedundantLabelChecksCodegenTest, unlabelledEndpointKeepsItsLabelCheck) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (a)-->(b)--(c:Person) RETURN a, c");

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);
}
