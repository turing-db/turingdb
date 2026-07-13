#include <gtest/gtest.h>

#include <memory>
#include <set>
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

using namespace db;
using namespace turing::test;

namespace {

// Names of properties registered for each entity type in a generated module.
struct RegisteredProps {
    std::multiset<std::string> nodeProps;
    std::multiset<std::string> edgeProps;
};

// Collects every GetNodeProperties and GetEdgeProperties op emitted by DBProgramGenerator.
RegisteredProps collectRegisteredProps(mlir::ModuleOp module) {
    RegisteredProps result;

    module.walk([&](mlir::db::GetNodeProperties op) {
        result.nodeProps.emplace(op.getProperty());
    });

    module.walk([&](mlir::db::GetEdgeProperties op) {
        result.edgeProps.emplace(op.getProperty());
    });

    return result;
}

}

class WhereExprRegistrationTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    // Generates a DB-dialect MLIR module from a Cypher query and collects
    // the property ops it contains.
    RegisteredProps generateAndCollect(std::string_view query) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule =
            mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        return collectRegisteredProps(module);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(WhereExprRegistrationTest, noWhereClause) {
    // No WHERE clause: no property ops should be generated.
    const RegisteredProps ops = generateAndCollect("MATCH (n) RETURN n");
    EXPECT_TRUE(ops.nodeProps.empty());
    EXPECT_TRUE(ops.edgeProps.empty());
}

// TODO: Enable when > supported
TEST_F(WhereExprRegistrationTest, DISABLED_singleNodeProp) {
    // WHERE n.age > 5 should produce exactly one GetNodeProperties for "age".
    const RegisteredProps ops = generateAndCollect("MATCH (n) WHERE n.age > 5 RETURN n");

    const std::multiset<std::string> expected = {"age"};
    EXPECT_EQ(ops.nodeProps, expected);
    EXPECT_TRUE(ops.edgeProps.empty());
}

// TODO: Enable when > supported
TEST_F(WhereExprRegistrationTest, DISABLED_twoNodePropsInAnd) {
    // WHERE n.age > 5 AND n.name = 'Remy' should produce GetNodeProperties for
    // both "age" and "name".
    const RegisteredProps ops = generateAndCollect(
        "MATCH (n) WHERE n.age > 5 AND n.name = 'Remy' RETURN n");

    const std::multiset<std::string> expected = {"age", "name"};
    EXPECT_EQ(ops.nodeProps, expected);
    EXPECT_TRUE(ops.edgeProps.empty());
}

// TODO: Enable when > supported
TEST_F(WhereExprRegistrationTest, DISABLED_threeNodePropsInNestedAnd) {
    // Three properties in a nested AND structure should each get their own op.
    const RegisteredProps ops = generateAndCollect(
        "MATCH (n) WHERE n.age > 5 AND n.isFrench = true AND n.hasPhD = true RETURN n");

    const std::multiset<std::string> expected = {"age", "hasPhD", "isFrench"};
    EXPECT_EQ(ops.nodeProps, expected);
    EXPECT_TRUE(ops.edgeProps.empty());
}

// TODO: Enable when > supported
TEST_F(WhereExprRegistrationTest, DISABLED_edgeProp) {
    // WHERE e.duration > 10 should produce exactly one GetEdgeProperties for "duration".
    const RegisteredProps ops = generateAndCollect(
        "MATCH (a)-[e]->(b) WHERE e.duration > 10 RETURN a");

    EXPECT_TRUE(ops.nodeProps.empty());
    const std::multiset<std::string> expected = {"duration"};
    EXPECT_EQ(ops.edgeProps, expected);
}

// TODO: Enable when > supported
TEST_F(WhereExprRegistrationTest, DISABLED_mixedNodeAndEdgeProps) {
    // WHERE referencing both a node property and an edge property should register both.
    const RegisteredProps ops = generateAndCollect(
        "MATCH (a)-[e]->(b) WHERE a.age > 5 AND e.duration > 10 RETURN a");

    const std::multiset<std::string> expectedNodeProps = {"age"};
    const std::multiset<std::string> expectedEdgeProps = {"duration"};
    EXPECT_EQ(ops.nodeProps, expectedNodeProps);
    EXPECT_EQ(ops.edgeProps, expectedEdgeProps);
}
