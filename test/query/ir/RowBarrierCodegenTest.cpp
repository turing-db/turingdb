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
class RowBarrierCodegenTest : public TuringTest {
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

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(RowBarrierCodegenTest, holdsTheRowsBeforeASetOfAPropertyTheMatchRead) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) WHERE n.age = 32 SET n.age = 33");
    EXPECT_EQ(countOps<mlir::db::RowBarrier>(*module), 1u);
}

TEST_F(RowBarrierCodegenTest, holdsTheRowsBeforeARemoveOfAPropertyTheMatchRead) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) WHERE n.age = 32 REMOVE n.age");
    EXPECT_EQ(countOps<mlir::db::RowBarrier>(*module), 1u);
}

TEST_F(RowBarrierCodegenTest, holdsTheRowsBeforeASetOfAPropertyAMergeLookedUp) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [1, 2] AS i MERGE (n:Person {name: 'Remy'}) SET n.name = 'X'");
    EXPECT_EQ(countOps<mlir::db::RowBarrier>(*module), 1u);
}

TEST_F(RowBarrierCodegenTest, holdsTheRowsBeforeADeleteOfWhatTheMatchWalked) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) DETACH DELETE n");
    EXPECT_EQ(countOps<mlir::db::RowBarrier>(*module), 1u);
}

TEST_F(RowBarrierCodegenTest, holdsTheRowsBeforeACreateOfWhatAMergeLooksUp) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [1, 2] AS i MERGE (t:Tally {k: 1}) CREATE (:Tally {k: 1})");
    EXPECT_EQ(countOps<mlir::db::RowBarrier>(*module), 1u);
}

TEST_F(RowBarrierCodegenTest, streamsASetOfAPropertyNothingRead) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) WHERE n.age = 32 SET n.name = 'X'");
    EXPECT_EQ(countOps<mlir::db::RowBarrier>(*module), 0u);
}

TEST_F(RowBarrierCodegenTest, streamsASetReadingItsOwnProperty) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) SET n.age = n.age + 1");
    EXPECT_EQ(countOps<mlir::db::RowBarrier>(*module), 0u);
}

TEST_F(RowBarrierCodegenTest, streamsACreateOfWhatNoMergeLooksUp) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [1, 2] AS i MERGE (t:Tally {k: 1}) CREATE (:Other {k: 1})");
    EXPECT_EQ(countOps<mlir::db::RowBarrier>(*module), 0u);
}

TEST_F(RowBarrierCodegenTest, streamsTwoMergesOfTheSameLabel) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("UNWIND [1, 2] AS i MERGE (a:Person {name: 'Remy'}) MERGE (b:Person {name: 'Adam'})");
    EXPECT_EQ(countOps<mlir::db::RowBarrier>(*module), 0u);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
