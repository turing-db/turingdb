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

#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBLowering.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLOps.h"
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
#include "list/ListElementView.h"
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Reads the first projected column as one optional integer per row, accepting both shapes
// an index result takes: a constant, when list and position are both literals, and a
// per-row nullable column otherwise.
class ElementSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const Column* column = chunks[0];

        const auto* constElement = dynamic_cast<const ColumnConst<std::optional<ListElementView>>*>(column);
        const auto* vectorElement = dynamic_cast<const ColumnOptVector<ListElementView>*>(column);
        ASSERT_TRUE(constElement || vectorElement);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            const std::optional<ListElementView> element =
                constElement ? (*constElement)[rowIndex] : vectorElement->getRaw()[rowIndex];

            if (!element.has_value()) {
                _rows.push_back(std::nullopt);
                continue;
            }

            _rows.push_back(element->getAs<types::Int64::Primitive>());
        }
    }

    const std::vector<std::optional<types::Int64::Primitive>>& rows() const { return _rows; }

private:
    std::vector<std::optional<types::Int64::Primitive>> _rows;
};

}

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

    void runQuery(std::string_view query, NLOutputSink* sink) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, view, procedures, context, module);

        LocalMemory memory;
        DBDialectInterpreter interpreter(module.get(), &view, sink, &memory);
        interpreter.run();
    }

    std::optional<types::Int64::Primitive> evalElement(std::string_view query) {
        ElementSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.rows().size(), 1u) << "query: " << query;
        return sink.rows().empty() ? std::nullopt : sink.rows().front();
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

TEST_F(ListIndexTest, readsTheElementAtAPosition) {
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][2]"), 4);
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][0]"), 1);
}

TEST_F(ListIndexTest, countsANegativePositionFromTheEnd) {
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][-1]"), 4);
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][-3]"), 1);
}

TEST_F(ListIndexTest, readsNullOutsideTheList) {
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][3]"), std::nullopt);
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][9]"), std::nullopt);
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][-4]"), std::nullopt);
}

TEST_F(ListIndexTest, readsAnElementOfAMixedList) {
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 'a', 4][0]"), 1);
}

// Remy is 32, so every position is past the end: the row's own value drives the read.
TEST_F(ListIndexTest, readsThePositionFromTheRow) {
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][n.age]"), std::nullopt);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
