#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnVector.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

class OwnedStringSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* strings = dynamic_cast<const ColumnVector<std::string>*>(chunks[0]);
        ASSERT_NE(strings, nullptr);

        const auto& raw = strings->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const std::vector<std::string>& values() const { return _values; }

private:
    std::vector<std::string> _values;
};

}

class OrderByLabelsTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, NLOutputSink* sink) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;

        {
            const FrozenCommitTx transaction = _graph->openTransaction();
            const GraphView view = transaction.viewGraph();

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

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        LocalMemory memory;
        DBDialectInterpreter interpreter(module.get(), &view, sink, &memory);
        interpreter.run();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(OrderByLabelsTest, ownedLabelStringSortsWithoutCrash) {
    OwnedStringSink sink;
    runQuery("MATCH (n) RETURN labels(n) ORDER BY labels(n)", &sink);

    const std::vector<std::string>& values = sink.values();

    EXPECT_EQ(values.size(), 18u);
    EXPECT_TRUE(std::is_sorted(values.begin(), values.end()));

    for (const std::string& value : values) {
        EXPECT_FALSE(value.empty());
    }
}
