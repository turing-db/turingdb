#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "llvm/Support/raw_ostream.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "CypherAnalyzer.h"
#include "CypherAST.h"
#include "CypherParser.h"

#include "DBDialect.h"
#include "DBPassPipeline.h"
#include "DBPasses.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

class DBPassPipelineReuseTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        _mlirContext.getOrLoadDialect<mlir::func::FuncDialect>();
        _mlirContext.getOrLoadDialect<mlir::storage::Storage>();
        _mlirContext.getOrLoadDialect<mlir::db::DB>();
        _mlirContext.getOrLoadDialect<mlir::nl::NL>();

        _passPipeline = std::make_unique<DBPassPipeline>(&_mlirContext);
    }

    void generate(std::string_view query, const mlir::db::DBPassContext& passContext, std::string& program) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        CypherAST ast(nullptr, query);
        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::OpBuilder builder(&_mlirContext);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module, nullptr, passContext);
        generator.setPassPipeline(_passPipeline.get());
        generator.generate(&ast);

        program.clear();
        llvm::raw_string_ostream stream(program);
        module.print(stream);
    }

    mlir::MLIRContext _mlirContext;
    std::unique_ptr<DBPassPipeline> _passPipeline;
    std::unique_ptr<Graph> _graph;
};

TEST_F(DBPassPipelineReuseTest, readsTheContextOfEachRun) {
    const std::string_view query = "MATCH (n:Person), (m:Person) WHERE n.name = m.name RETURN n, m";
    const mlir::db::DBPassContext fusing {nullptr, false, true};
    const mlir::db::DBPassContext notFusing {nullptr, false, false};

    std::string program;
    generate(query, fusing, program);
    EXPECT_NE(program.find("db.hash_join"), std::string::npos) << program;

    generate(query, notFusing, program);
    EXPECT_EQ(program.find("db.hash_join"), std::string::npos) << program;

    generate(query, fusing, program);
    EXPECT_NE(program.find("db.hash_join"), std::string::npos) << program;
}

TEST_F(DBPassPipelineReuseTest, dropsTheViewOfARunWhenItEnds) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const mlir::db::DBPassContext passContext {._view = &view};

    std::string program;
    generate("MATCH (n:Person) RETURN count(n)", passContext, program);

    const mlir::db::DBPassContext& retainedContext = _passPipeline->getPassContext();
    EXPECT_EQ(retainedContext._view, nullptr);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
