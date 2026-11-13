#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

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
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestOps.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class ComputedColumnTypeTest : public TuringTest {
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
    template <typename OpType>
    void expectResultTypes(std::string_view query, const std::vector<std::string>& expected) {
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
        const mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        std::vector<std::string> resultTypes;
        for (OpType op : collect<OpType>(module)) {
            std::string type;
            llvm::raw_string_ostream stream(type);
            op->getResult(0).getType().print(stream);
            resultTypes.push_back(type);
        }

        std::string program;
        llvm::raw_string_ostream programStream(program);
        module.print(programStream);

        EXPECT_EQ(resultTypes, expected) << query << "\n" << program;
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(ComputedColumnTypeTest, declaresTheNumberAnOperatorComputes) {
    expectResultTypes<mlir::db::AddOp>("MATCH (n:Person) RETURN n.age + 1",
                                       {"!db.column<!storage.nullable<i64>>"});
    expectResultTypes<mlir::db::MulOp>("MATCH (n:Person) RETURN n.age * 2.5",
                                       {"!db.column<!storage.nullable<f64>>"});
}

TEST_F(ComputedColumnTypeTest, declaresWhatAFunctionComputesOverAnother) {
    const std::string_view query = "MATCH (n:Person) RETURN size(toUpper(n.name))";

    expectResultTypes<mlir::db::ToUpper>(query, {"!db.column<!storage.nullable<!storage.string>>"});
    expectResultTypes<mlir::db::Size>(query, {"!db.column<!storage.nullable<i64>>"});
}

TEST_F(ComputedColumnTypeTest, declaresEveryLinkOfAChain) {
    expectResultTypes<mlir::db::AddOp>("MATCH (n:Person) WITH n.age AS v WITH v + 1 AS v WITH v + 1 AS v RETURN v",
                                       {"!db.column<!storage.nullable<i64>>", "!db.column<!storage.nullable<i64>>"});
}

TEST_F(ComputedColumnTypeTest, declaresNoTypeOverACell) {
    expectResultTypes<mlir::db::AddOp>("UNWIND [1, 'a'] AS x RETURN x + 1", {"!db.column<none>"});
}
