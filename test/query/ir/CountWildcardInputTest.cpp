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

// count(*) counts the rows the query matched, so its input is a column holding them: the
// one the query's first variable is bound to where the count is emitted. Reading a column
// out of a pointer-keyed map instead leaves both the variable and the block its column was
// bound in to the addresses the variables happen to sit at.
class CountWildcardInputTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    // Parses, analyzes and generates the db dialect program of a query into @ref _module,
    // without running it
    void generateProgram(std::string_view query) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        _ast = std::make_unique<CypherAST>(procedures, query);
        CypherAST* ast = _ast.get();

        CypherParser parser(ast);
        parser.parse(query);

        CypherAnalyzer analyzer(ast, view);
        analyzer.setV3();
        analyzer.analyze();

        mlir::MLIRContext* context = &_context;
        context->getOrLoadDialect<mlir::func::FuncDialect>();
        context->getOrLoadDialect<mlir::storage::Storage>();
        context->getOrLoadDialect<mlir::db::DB>();
        context->getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(context);
        _module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = _module.get();

        _generator = std::make_unique<DBProgramGenerator>(&module);
        _generator->generate(ast);
    }

    // The single cross product of the generated program - the one pairing the two patterns
    mlir::db::CrossProduct findCrossProduct() {
        mlir::db::CrossProduct product;
        _module.get().walk([&](mlir::db::CrossProduct found) { product = found; });

        return product;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};

    std::unique_ptr<CypherAST> _ast;
    mlir::MLIRContext _context;
    mlir::OwningOpRef<mlir::ModuleOp> _module;
    std::unique_ptr<DBProgramGenerator> _generator;
};

// The two patterns are two components, so a product pairs them and its results are the
// column of a followed by the column of b. The count takes the first of them - the column
// of the query's first variable - and takes it where the product bound it, which is the
// block the count is emitted into.
TEST_F(CountWildcardInputTest, countsTheColumnOfTheFirstMatchedVariable) {
    generateProgram("MATCH (a), (b) RETURN count(*)");

    mlir::db::Count count;
    _module.get().walk([&](mlir::db::Count found) { count = found; });
    ASSERT_TRUE(count);

    mlir::db::CrossProduct product = findCrossProduct();
    ASSERT_TRUE(product);

    const mlir::Value counted = count.getInput();
    EXPECT_EQ(counted, product.getResult(0));
    EXPECT_EQ(counted.getDefiningOp()->getBlock(), count->getBlock());
}

// The same choice on the grouped path: the aggregate input follows the grouping keys in the
// operands, and it is the column of a - the product's first result - while b is the key.
TEST_F(CountWildcardInputTest, groupedAggregateCountsTheColumnOfTheFirstMatchedVariable) {
    generateProgram("MATCH (a), (b) RETURN b, count(*)");

    mlir::db::GroupAggregate groupAggregate;
    _module.get().walk([&](mlir::db::GroupAggregate found) { groupAggregate = found; });
    ASSERT_TRUE(groupAggregate);

    mlir::db::CrossProduct product = findCrossProduct();
    ASSERT_TRUE(product);

    const mlir::OperandRange columns = groupAggregate.getColumns();
    ASSERT_EQ(columns.size(), 2u);
    EXPECT_EQ(groupAggregate.getKeyCount(), 1u);

    const mlir::Value key = columns[0];
    const mlir::Value counted = columns[1];

    EXPECT_EQ(key, product.getResult(1));
    EXPECT_EQ(counted, product.getResult(0));
    EXPECT_EQ(counted.getDefiningOp()->getBlock(), groupAggregate->getBlock());
}
