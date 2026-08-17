#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <span>
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
#include "columns/ColumnConst.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Reads the first projected column - the arithmetic result - as a double. Modulo of two
// integers yields an int64 constant column; exponentiation always yields a double column
// per openCypher. Both broadcast, so the value at any subscript is the operation's result.
class ScalarSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const Column* column = chunks[0];
        const auto* integers = dynamic_cast<const ColumnConst<int64_t>*>(column);
        const auto* doubles = dynamic_cast<const ColumnConst<double>*>(column);
        ASSERT_TRUE(integers || doubles);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            if (integers) {
                _values.push_back(static_cast<double>((*integers)[rowIndex]));
            } else {
                _values.push_back((*doubles)[rowIndex]);
            }
        }
    }

    const std::vector<double>& values() const { return _values; }

private:
    std::vector<double> _values;
};

}

class ArithmeticExtTest : public TuringTest {
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
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);

        LocalMemory memory;
        DBDialectInterpreter interpreter(moduleOp, &view, sink, &memory);
        interpreter.run();
    }

    double evalScalar(std::string_view query) {
        ScalarSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.values().size(), 1u) << "query: " << query;
        return sink.values().empty() ? 0.0 : sink.values().front();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(ArithmeticExtTest, integerModulo) {
    EXPECT_EQ(evalScalar("MATCH (n) WHERE n.name = 'Remy' RETURN 10 % 3, n.name"), 1.0);
    EXPECT_EQ(evalScalar("MATCH (n) WHERE n.name = 'Remy' RETURN 10 % 4, n.name"), 2.0);
}

TEST_F(ArithmeticExtTest, doubleModulo) {
    EXPECT_DOUBLE_EQ(evalScalar("MATCH (n) WHERE n.name = 'Remy' RETURN 5.5 % 2.0, n.name"), 1.5);
}

TEST_F(ArithmeticExtTest, exponentiationYieldsDouble) {
    EXPECT_DOUBLE_EQ(evalScalar("MATCH (n) WHERE n.name = 'Remy' RETURN 2 ^ 10, n.name"), 1024.0);
    EXPECT_DOUBLE_EQ(evalScalar("MATCH (n) WHERE n.name = 'Remy' RETURN 9 ^ 0.5, n.name"), 3.0);
}

TEST_F(ArithmeticExtTest, moduloByZeroThrows) {
    ScalarSink integerSink;
    EXPECT_THROW(runQuery("MATCH (n) WHERE n.name = 'Remy' RETURN 10 % 0, n.name", &integerSink),
                 TuringException);

    ScalarSink doubleSink;
    EXPECT_THROW(runQuery("MATCH (n) WHERE n.name = 'Remy' RETURN 5.5 % 0.0, n.name", &doubleSink),
                 TuringException);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
