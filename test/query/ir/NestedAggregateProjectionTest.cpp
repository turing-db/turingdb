#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
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

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

const std::string_view nestedAggregateReason = "Nested aggregates are not supported";

}

// An item carrying an aggregate is not itself the aggregate function: {total: count(n)}
// and count(n) + 1 are both flagged aggregate without being a function invocation
class NestedAggregateProjectionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    // Parses, analyzes and generates the db dialect program of a query, without running it
    void generateProgram(std::string_view query) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.setV3();
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
    }

    // The query is turned away, and on @param reason rather than on anything the parser or
    // the analyzer may have had to say about it first
    void expectRejected(std::string_view query, std::string_view reason) {
        try {
            generateProgram(query);
        } catch (const TuringException& error) {
            const std::string message = error.what();
            EXPECT_NE(message.find(reason), std::string::npos)
                << "query: " << query << "\nerror: " << message;
            return;
        }

        ADD_FAILURE() << "query was accepted: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// The same count over the same group, written where codegen can build it: one grouping
// key and one aggregate is a grouped aggregate, and it must keep generating - what the
// tests below reject is where the aggregate sits, not the grouping
TEST_F(NestedAggregateProjectionTest, generatesTheSameAggregateOutsideAMap) {
    EXPECT_NO_THROW(generateProgram("MATCH (n:Person) RETURN n.name, count(n)"));
}

// A map holding a constant next to the same grouping key: nothing here aggregates, so the
// map is turned away where every literal codegen has no column for is. This is the
// outcome the aggregate spelling has to match.
TEST_F(NestedAggregateProjectionTest, rejectsAMapHoldingAConstant) {
    EXPECT_THROW(generateProgram("MATCH (n:Person) RETURN n.name, {total: 1}"), TuringException);
}

// The map on its own leaves the projection with no grouping key, so the grouped aggregate
// codegen does not run and the map is turned away on its own account
TEST_F(NestedAggregateProjectionTest, rejectsAMapHoldingAnAggregateAlone) {
    EXPECT_THROW(generateProgram("MATCH (n:Person) RETURN {total: count(n)}"), TuringException);
}

// The finding: the aggregate the map holds is propagated onto the map literal itself, so
// the projection reads as one grouping key plus one aggregate and the grouped aggregate
// codegen runs. It reaches the map literal through a FunctionInvocationExpr* it casts
// unchecked, and reads a MapLiteral* as a FunctionInvocation*. The query names a value
// codegen cannot build, so it must be turned away like the two above - not crashed on.
TEST_F(NestedAggregateProjectionTest, rejectsAMapHoldingAnAggregateBesideAGroupingKey) {
    expectRejected("MATCH (n:Person) RETURN n.name, {total: count(n)}", nestedAggregateReason);
}

TEST_F(NestedAggregateProjectionTest, rejectsAnAggregateInsideAnExpression) {
    expectRejected("MATCH (n:Person) RETURN n.name, count(n) + 1", nestedAggregateReason);
}
