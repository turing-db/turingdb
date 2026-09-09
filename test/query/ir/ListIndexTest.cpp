#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "NLOps.h"
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

class ListIndexTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void generateProgram(std::string_view query,
                         const GraphView& view,
                         const ProcedureManager* procedures,
                         mlir::MLIRContext& context,
                         mlir::OwningOpRef<mlir::ModuleOp>& module) {
        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.setV3();
        analyzer.analyze();

        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);
    }

    std::string indexResultType(std::string_view query) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, view, procedures, context, module);

        const mlir::func::FuncOp dbFunction = module.get().lookupSymbol<mlir::func::FuncOp>("main");
        EXPECT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view);
        lowering.lower(dbFunction, *nlModule);

        mlir::nl::Index index;
        nlModule->walk([&](mlir::nl::Index op) { index = op; });
        EXPECT_TRUE(index);

        std::string printed;
        llvm::raw_string_ostream stream(printed);
        index.getResult().getType().getElementType().print(stream);

        return printed;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(ListIndexTest, typesAnIndexAsANullableTaggedScalar) {
    const std::string resultType = indexResultType("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 2, 3][1]");

    EXPECT_NE(resultType.find("nullable"), std::string::npos) << "index result type: " << resultType;
    EXPECT_NE(resultType.find("list_element"), std::string::npos) << "index result type: " << resultType;
}

// A homogeneous list gives the access no more specific type than a mixed one does: the
// element read carries its own tag either way.
TEST_F(ListIndexTest, typesAnIndexIntoAMixedListTheSameWay) {
    const std::string homogeneous = indexResultType("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 2, 3][1]");
    const std::string mixed = indexResultType("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 'a'][1]");

    EXPECT_EQ(homogeneous, mixed);
}

// Out of range reads null, so the result is nullable however the index is written.
TEST_F(ListIndexTest, typesANegativeIndexTheSameWay) {
    const std::string forward = indexResultType("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 2, 3][1]");
    const std::string fromTheEnd = indexResultType("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 2, 3][-1]");

    EXPECT_EQ(forward, fromTheEnd);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
