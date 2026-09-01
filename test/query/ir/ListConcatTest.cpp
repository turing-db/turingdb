#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <span>
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
#include "columns/ColumnVector.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Reads the first projected column as one integer list per row, accepting the shapes a
// list concatenation can produce: a broadcast constant (two literals) or a plain view
// column.
class ListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const Column* column = chunks[0];

        const auto* constView = dynamic_cast<const ColumnConst<ListView>*>(column);
        const auto* vectorView = dynamic_cast<const ColumnVector<ListView>*>(column);
        ASSERT_TRUE(constView || vectorView);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            const ListView list = constView ? (*constView)[rowIndex] : vectorView->getRaw()[rowIndex];

            std::vector<types::Int64::Primitive> elements;
            for (const ListElementView element : list) {
                elements.push_back(element.getAs<types::Int64::Primitive>());
            }

            _rows.push_back(elements);
        }
    }

    const std::vector<std::vector<types::Int64::Primitive>>& rows() const { return _rows; }

private:
    std::vector<std::vector<types::Int64::Primitive>> _rows;
};

}

class ListConcatTest : public TuringTest {
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

    std::vector<types::Int64::Primitive> evalList(std::string_view query) {
        ListSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.rows().size(), 1u) << "query: " << query;
        return sink.rows().empty() ? std::vector<types::Int64::Primitive> {} : sink.rows().front();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(ListConcatTest, twoLiterals) {
    const std::vector<types::Int64::Primitive> expected {1, 2, 3, 4};
    EXPECT_EQ(evalList("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 2] + [3, 4]"), expected);
}

TEST_F(ListConcatTest, singletons) {
    const std::vector<types::Int64::Primitive> expected {1, 2};
    EXPECT_EQ(evalList("MATCH (n) WHERE n.name = 'Remy' RETURN [1] + [2]"), expected);
}

TEST_F(ListConcatTest, chainedConcatenation) {
    const std::vector<types::Int64::Primitive> expected {1, 2, 3};
    EXPECT_EQ(evalList("MATCH (n) WHERE n.name = 'Remy' RETURN [1] + [2] + [3]"), expected);
}

TEST_F(ListConcatTest, subtractionOfListsRejected) {
    ListSink sink;
    EXPECT_THROW(runQuery("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 2] - [3, 4]", &sink),
                 TuringException);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
