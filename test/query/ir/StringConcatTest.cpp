#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <optional>
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
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Reads the first projected column as strings, accepting the shapes a concatenation can
// produce: a broadcast constant (two literals), a plain view column, or a nullable view
// column (when a nullable property is an operand).
class StringSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const Column* column = chunks[0];

        const auto* constView = dynamic_cast<const ColumnConst<std::string_view>*>(column);
        const auto* vectorView = dynamic_cast<const ColumnVector<std::string_view>*>(column);
        const auto* optView = dynamic_cast<const ColumnOptVector<std::string_view>*>(column);
        ASSERT_TRUE(constView || vectorView || optView);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            if (constView) {
                _values.emplace_back((*constView)[rowIndex]);
            } else if (vectorView) {
                _values.emplace_back(vectorView->getRaw()[rowIndex]);
            } else {
                const std::optional<std::string_view>& value = optView->getRaw()[rowIndex];
                _values.push_back(value.has_value() ? std::string(*value) : std::string("<null>"));
            }
        }
    }

    const std::vector<std::string>& values() const { return _values; }

private:
    std::vector<std::string> _values;
};

}

class StringConcatTest : public TuringTest {
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

    std::string evalString(std::string_view query) {
        StringSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.values().size(), 1u) << "query: " << query;
        return sink.values().empty() ? std::string() : sink.values().front();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(StringConcatTest, twoLiterals) {
    EXPECT_EQ(evalString("MATCH (n) WHERE n.name = 'Remy' RETURN 'foo' + 'bar'"), "foobar");
}

TEST_F(StringConcatTest, propertyPlusLiteral) {
    EXPECT_EQ(evalString("MATCH (n) WHERE n.name = 'Remy' RETURN n.name + '!'"), "Remy!");
}

TEST_F(StringConcatTest, literalPlusProperty) {
    EXPECT_EQ(evalString("MATCH (n) WHERE n.name = 'Remy' RETURN 'Hello, ' + n.name"), "Hello, Remy");
}

TEST_F(StringConcatTest, chainedConcatenation) {
    EXPECT_EQ(evalString("MATCH (n) WHERE n.name = 'Remy' RETURN n.name + ' ' + n.name"), "Remy Remy");
}

TEST_F(StringConcatTest, subtractionOfStringsRejected) {
    StringSink sink;
    EXPECT_THROW(runQuery("MATCH (n) WHERE n.name = 'Remy' RETURN n.name - '!'", &sink),
                 TuringException);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
