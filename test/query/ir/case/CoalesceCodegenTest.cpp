#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "ExplainReport.h"
#include "ExplainRequest.h"
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

// coalesce of k arguments is one db.coalesce holding k regions, the regions in the order of
// the arguments. The passes drop the regions yielding the null literal, and a coalesce left
// with one region gives way to the value that region computes.
class CoalesceCodegenTest : public TuringTest {
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
    // The program codegen emits for @param query, read back from its dump, before any pass
    mlir::OwningOpRef<mlir::ModuleOp> generateUnoptimised(std::string_view query) {
        ExplainRequest request;
        request.requestStage("codegen");
        ExplainReport report(&request);

        mlir::OwningOpRef<mlir::ModuleOp> generated = generate(query, &report);
        if (!generated || report.getDumps().size() != 1) {
            return {};
        }

        return mlir::parseSourceString<mlir::ModuleOp>(report.getDumps().front(), mlir::ParserConfig(&_context));
    }

    // The program the passes leave for @param query
    mlir::OwningOpRef<mlir::ModuleOp> generate(std::string_view query, ExplainReport* report = nullptr) {
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

        DBProgramGenerator generator(&module, report);
        generator.generate(&ast);

        return owningModule;
    }

    // The one coalesce of @param module
    mlir::db::Coalesce onlyCoalesce(mlir::ModuleOp module) {
        llvm::SmallVector<mlir::db::Coalesce> coalesces = collect<mlir::db::Coalesce>(module);
        if (coalesces.size() != 1) {
            ADD_FAILURE() << "expected one db.coalesce, found " << coalesces.size();
            return {};
        }

        return coalesces.front();
    }

    // The op computing what region @param regionIndex of @param coalesce yields
    mlir::Operation* yieldingOp(mlir::db::Coalesce coalesce, size_t regionIndex) {
        mlir::Block& body = coalesce.getArguments()[regionIndex].front();
        mlir::db::CaseYield yield = mlir::cast<mlir::db::CaseYield>(body.getTerminator());

        return yield.getValue().getDefiningOp();
    }

    // The property a region yields the read of, empty when it yields no property read
    llvm::StringRef yieldedProperty(mlir::db::Coalesce coalesce, size_t regionIndex) {
        mlir::db::GetNodeProperties read = mlir::dyn_cast_or_null<mlir::db::GetNodeProperties>(yieldingOp(coalesce, regionIndex));
        if (!read) {
            return {};
        }

        return read.getProperty();
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(CoalesceCodegenTest, emitsOneRegionPerArgumentInTheirOrder) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generateUnoptimised(
        "UNWIND [0, 1, 2] AS t RETURN coalesce(CASE WHEN t = 0 THEN -1 END, CASE WHEN t = 1 THEN -2 END, 10 / t)");
    ASSERT_TRUE(module);

    mlir::db::Coalesce coalesce = onlyCoalesce(*module);
    ASSERT_TRUE(coalesce);
    ASSERT_EQ(coalesce.getArguments().size(), 3u);

    EXPECT_TRUE(mlir::isa_and_nonnull<mlir::db::LazyCase>(yieldingOp(coalesce, 0)));
    EXPECT_TRUE(mlir::isa_and_nonnull<mlir::db::LazyCase>(yieldingOp(coalesce, 1)));
    EXPECT_TRUE(mlir::isa_and_nonnull<mlir::db::DivOp>(yieldingOp(coalesce, 2)));

    for (mlir::db::LazyCase caseOp : collect<mlir::db::LazyCase>(*module)) {
        EXPECT_TRUE(coalesce->isProperAncestor(caseOp.getOperation()));
    }
}

TEST_F(CoalesceCodegenTest, emitsOneRegionForOneArgument) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generateUnoptimised("MATCH (n:Person) RETURN coalesce(n.age)");
    ASSERT_TRUE(module);

    mlir::db::Coalesce coalesce = onlyCoalesce(*module);
    ASSERT_TRUE(coalesce);
    ASSERT_EQ(coalesce.getArguments().size(), 1u);
    EXPECT_EQ(yieldedProperty(coalesce, 0), "age");
}

TEST_F(CoalesceCodegenTest, emitsARegionForEveryNullLiteral) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generateUnoptimised("MATCH (n:Person) RETURN coalesce(null, n.age, null, n.name, null)");
    ASSERT_TRUE(module);

    mlir::db::Coalesce coalesce = onlyCoalesce(*module);
    ASSERT_TRUE(coalesce);
    ASSERT_EQ(coalesce.getArguments().size(), 5u);
    EXPECT_EQ(yieldedProperty(coalesce, 1), "age");
    EXPECT_EQ(yieldedProperty(coalesce, 3), "name");
}

TEST_F(CoalesceCodegenTest, dropsTheRegionsYieldingTheNullLiteral) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (n:Person) RETURN coalesce(null, n.age, null, n.name, null)");
    ASSERT_TRUE(module);

    mlir::db::Coalesce coalesce = onlyCoalesce(*module);
    ASSERT_TRUE(coalesce);
    ASSERT_EQ(coalesce.getArguments().size(), 2u);
    EXPECT_EQ(yieldedProperty(coalesce, 0), "age");
    EXPECT_EQ(yieldedProperty(coalesce, 1), "name");
}

TEST_F(CoalesceCodegenTest, givesWayToTheOneArgumentLeft) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) RETURN coalesce(null, n.name, null)");
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::Coalesce>(*module), 0u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);

    mlir::db::GetNodeProperties read = outputs.front().getColumns().front().getDefiningOp<mlir::db::GetNodeProperties>();
    ASSERT_TRUE(read);
    EXPECT_EQ(read.getProperty(), "name");
}

TEST_F(CoalesceCodegenTest, givesWayToItsOnlyArgument) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) RETURN coalesce(n.age)");
    ASSERT_TRUE(module);

    EXPECT_EQ(countOps<mlir::db::Coalesce>(*module), 0u);
}

TEST_F(CoalesceCodegenTest, dropsTheRegionsAfterALiteralThatIsNotNull) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) RETURN coalesce(n.dob, 'x', n.name)");
    ASSERT_TRUE(module);

    mlir::db::Coalesce coalesce = onlyCoalesce(*module);
    ASSERT_TRUE(coalesce);
    ASSERT_EQ(coalesce.getArguments().size(), 2u);
    EXPECT_EQ(yieldedProperty(coalesce, 0), "dob");
    EXPECT_TRUE(mlir::isa_and_nonnull<mlir::db::ConstantOp>(yieldingOp(coalesce, 1)));
}

// An integer beside a string leaves the result's element to lowering, which reads it off
// every argument, the ones behind the literal included
TEST_F(CoalesceCodegenTest, keepsTheRegionsAfterALiteralWhereTheResultNamesNoElement) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = generate("MATCH (n:Person) RETURN coalesce(n.age, 7, n.name)");
    ASSERT_TRUE(module);

    mlir::db::Coalesce coalesce = onlyCoalesce(*module);
    ASSERT_TRUE(coalesce);
    EXPECT_EQ(coalesce.getArguments().size(), 3u);
}
