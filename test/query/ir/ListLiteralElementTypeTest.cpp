#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "DBTypes.h"
#include "NLDialect.h"
#include "StorageDialect.h"
#include "StorageTypes.h"

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

namespace {

// A type spelled as MLIR prints it, so a mismatch names the two types rather than dumping
// the bytes of their handles.
std::string typeText(mlir::Type type) {
    std::string text;
    llvm::raw_string_ostream stream(text);
    type.print(stream);

    return text;
}

}

// The element type db.const_list is given is the homogeneity verdict codegen reached, and
// it is the only place that verdict is visible: the runtime column is a ColumnConst of
// ListViews either way and every cell keeps its own type tag, so a list typed as i64 and
// one typed as list_element render the same rows. These tests read the type off the
// generated op rather than the rows, so a wrong verdict cannot pass unnoticed.
class ListLiteralElementTypeTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);

        // A storage type can only be built once its dialect is loaded, and a test spells
        // the type it expects before generating anything, so the context is loaded here
        // rather than alongside the generator.
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    void generateProgram(std::string_view query, mlir::OwningOpRef<mlir::ModuleOp>& module) {
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

        mlir::OpBuilder builder(&_context);
        module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);
    }

    void expectListElementType(std::string_view query, mlir::Type expected) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, module);

        mlir::db::ConstList constList;
        module.get().walk([&](mlir::db::ConstList op) {
            constList = op;
        });
        ASSERT_TRUE(constList) << "query: " << query << " generated no db.const_list";

        const mlir::db::ColumnType column = mlir::cast<mlir::db::ColumnType>(constList.getResult().getType());
        const mlir::storage::ListType listType = mlir::dyn_cast<mlir::storage::ListType>(column.getType());
        ASSERT_TRUE(listType) << "query: " << query << " gave " << typeText(column);

        const mlir::Type elementType = listType.getElementType();
        EXPECT_EQ(elementType, expected) << "query: " << query
                                         << " gave a list of " << typeText(elementType)
                                         << ", expected " << typeText(expected);
    }

    mlir::Type listElementType() { return mlir::storage::ListElementType::get(&_context); }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(ListLiteralElementTypeTest, typesAnIntegerListAsIntegers) {
    expectListElementType("RETURN [1, 2, 3]", mlir::IntegerType::get(&_context, 64));
}

TEST_F(ListLiteralElementTypeTest, typesAStringListAsStrings) {
    expectListElementType("RETURN ['one', 'two']", mlir::storage::StringType::get(&_context));
}

TEST_F(ListLiteralElementTypeTest, typesADoubleListAsDoubles) {
    expectListElementType("RETURN [1.5, 2.5]", mlir::Float64Type::get(&_context));
}

TEST_F(ListLiteralElementTypeTest, typesABoolListAsBools) {
    // A boolean literal rides an i1 attribute, so a list of them is a list of i1.
    expectListElementType("RETURN [true, false]", mlir::IntegerType::get(&_context, 1));
}

TEST_F(ListLiteralElementTypeTest, typesASingleElementListAsThatElement) {
    // One element is enough to read a type from - the boundary of the rule that a typed
    // list carries at least one element.
    expectListElementType("RETURN [1]", mlir::IntegerType::get(&_context, 64));
}

TEST_F(ListLiteralElementTypeTest, typesAMixedListAsTaggedScalars) {
    expectListElementType("RETURN [true, 'mixed', 10]", listElementType());
}

TEST_F(ListLiteralElementTypeTest, typesAListEndingInNullAsTaggedScalars) {
    // A null rides a unit attribute, which carries no type, so it agrees with no element:
    // the integer ahead of it does not make the list a list of integers.
    expectListElementType("RETURN [1, null]", listElementType());
}

TEST_F(ListLiteralElementTypeTest, typesAListOpeningOnNullAsTaggedScalars) {
    // The same verdict with the null first, where there is no type yet to disagree with -
    // the elements are read for a shared type from the front, so this is the other way in.
    expectListElementType("RETURN [null, 1]", listElementType());
}

TEST_F(ListLiteralElementTypeTest, typesASingletonNullListAsTaggedScalars) {
    // Nothing but the null, so the only element is the untyped one.
    expectListElementType("RETURN [null]", listElementType());
}

TEST_F(ListLiteralElementTypeTest, typesAMixedNumericListAsTaggedScalars) {
    // An integer and a float share no single type - the same exact-type rule UNWIND
    // follows, with no promotion to a list of doubles.
    expectListElementType("RETURN [1, 2.5]", listElementType());
}

TEST_F(ListLiteralElementTypeTest, typesAnEmptyListAsTaggedScalars) {
    // There is no element to read a type from, so the empty list takes the type-erased
    // form rather than an arbitrary one.
    expectListElementType("RETURN []", listElementType());
}

TEST_F(ListLiteralElementTypeTest, typesAListHoldingAListAsTaggedScalars) {
    // A nested list rides an array attribute, which carries no type, so a list holding one
    // is type-erased however its other elements agree.
    expectListElementType("RETURN [10, true, [1, 2]]", listElementType());
}

TEST_F(ListLiteralElementTypeTest, typesAListOfListsAsTaggedScalars) {
    // Every element is a list here, so they do agree - but on a type none of them carries,
    // which is the type-erased form again.
    expectListElementType("RETURN [[1, 2], [3]]", listElementType());
}

TEST_F(ListLiteralElementTypeTest, typesAListBesideAMatchedRowAsIntegers) {
    // The verdict is the literals' alone: it does not change with what the list is
    // projected beside.
    expectListElementType("MATCH (n) RETURN n.name, [1, 2, 3]", mlir::IntegerType::get(&_context, 64));
}
