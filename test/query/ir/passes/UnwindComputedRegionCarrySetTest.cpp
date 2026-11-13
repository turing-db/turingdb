#include <gtest/gtest.h>

#include <stddef.h>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "DBRowSource.h"
#include "StorageDialect.h"

#include "IRTestOps.h"

using namespace turing::test;

// A db.unwind_computed carries its columns into its region and hands them back as its results
// after the element, so a pass reading the region layout through the interface sees through it
class UnwindComputedRegionCarrySetTest : public ::testing::Test {
protected:
    UnwindComputedRegionCarrySetTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runTrim(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createTrimUnreadColumns());

        return mlir::succeeded(passManager.run(module)) && mlir::succeeded(mlir::verify(module));
    }

    bool runReuse(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createReusePropertyReads());

        return mlir::succeeded(passManager.run(module)) && mlir::succeeded(mlir::verify(module));
    }

    static mlir::db::UnwindComputed theUnwind(mlir::ModuleOp module) {
        llvm::SmallVector<mlir::db::UnwindComputed> unwinds = collect<mlir::db::UnwindComputed>(module);
        EXPECT_EQ(unwinds.size(), 1u);

        return unwinds.empty() ? mlir::db::UnwindComputed() : unwinds.front();
    }

    mlir::MLIRContext _context;
};

// MATCH (a) WITH a, a.age AS age UNWIND [a.name] AS x RETURN x, a
const char* const unwindCarryingTwoColumns = R"mlir(
!nodes = !db.column<!storage.node_id>
!names = !db.column<!storage.nullable<!storage.string>>
!ages = !db.column<!storage.nullable<i64>>
!nameLists = !db.column<!storage.list<!storage.nullable<!storage.string>>>
!cells = !db.column<!storage.any>

func.func @main() {
  %a = db.scan_nodes() : !nodes
  %age = db.get_node_properties(%a, "age") : (!nodes) -> !ages
  %x, %ac, %agec = db.unwind_computed({%a, %age}) {
  ^bb0(%in: !nodes, %ageIn: !ages):
    %name = db.get_node_properties(%in, "name") : (!nodes) -> !names
    %names = db.make_list(%name) : (!names) -> !nameLists
    db.unwind_yield %names : !nameLists
  } : (!nodes, !ages) -> (!cells, !nodes, !ages)
  db.output(%x, %ac) : !cells, !nodes
  return
}
)mlir";

TEST_F(UnwindComputedRegionCarrySetTest, takesEachArgumentFromTheColumnItCarries) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unwindCarryingTwoColumns);
    ASSERT_TRUE(module);

    mlir::db::UnwindComputed unwind = theUnwind(*module);
    ASSERT_TRUE(unwind);

    mlir::Block& body = unwind.getSource().front();
    const mlir::OperandRange carried = unwind.getColumnsToFilter();
    ASSERT_EQ(body.getNumArguments(), carried.size());

    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        const mlir::Value argument = body.getArgument(static_cast<unsigned>(carriedIndex));
        EXPECT_EQ(mlir::db::rowSourceOf(argument), carried[carriedIndex]) << carriedIndex;
    }
}

TEST_F(UnwindComputedRegionCarrySetTest, dropsAColumnNeitherItsRegionNorALaterOpReads) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unwindCarryingTwoColumns);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));

    mlir::db::UnwindComputed unwind = theUnwind(*module);
    ASSERT_TRUE(unwind);

    ASSERT_EQ(unwind.getColumnsToFilter().size(), 1u);
    EXPECT_TRUE(mlir::isa_and_present<mlir::db::ScanNodes>(unwind.getColumnsToFilter().front().getDefiningOp()));
    EXPECT_EQ(unwind.getSource().front().getNumArguments(), 1u);
    EXPECT_EQ(unwind->getNumResults(), 2u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs.front()->getOperand(0), unwind.getElement());
    EXPECT_EQ(outputs.front()->getOperand(1), unwind.getCarried().front());

    EXPECT_EQ(collect<mlir::db::GetNodeProperties>(*module).size(), 1u);
}

// MATCH (a) WITH a, a.age AS age UNWIND [a.name] AS x RETURN x, age
const char* const unwindHandingBackItsSecondColumn = R"mlir(
!nodes = !db.column<!storage.node_id>
!names = !db.column<!storage.nullable<!storage.string>>
!ages = !db.column<!storage.nullable<i64>>
!nameLists = !db.column<!storage.list<!storage.nullable<!storage.string>>>
!cells = !db.column<!storage.any>

func.func @main() {
  %a = db.scan_nodes() : !nodes
  %age = db.get_node_properties(%a, "age") : (!nodes) -> !ages
  %x, %ac, %agec = db.unwind_computed({%a, %age}) {
  ^bb0(%in: !nodes, %ageIn: !ages):
    %name = db.get_node_properties(%in, "name") : (!nodes) -> !names
    %names = db.make_list(%name) : (!names) -> !nameLists
    db.unwind_yield %names : !nameLists
  } : (!nodes, !ages) -> (!cells, !nodes, !ages)
  db.output(%x, %agec) : !cells, !ages
  return
}
)mlir";

TEST_F(UnwindComputedRegionCarrySetTest, keepsAColumnALaterOpReadsAfterTheElement) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unwindHandingBackItsSecondColumn);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runTrim(*module));

    mlir::db::UnwindComputed unwind = theUnwind(*module);
    ASSERT_TRUE(unwind);

    ASSERT_EQ(unwind.getColumnsToFilter().size(), 2u);
    EXPECT_EQ(unwind->getNumResults(), 3u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs.front()->getOperand(1), unwind.getCarried()[1]);
}

// MATCH (a) WITH a, a.name AS name UNWIND [a.name] AS x RETURN x, name
const char* const unwindReadingACarriedProperty = R"mlir(
!nodes = !db.column<!storage.node_id>
!names = !db.column<!storage.nullable<!storage.string>>
!nameLists = !db.column<!storage.list<!storage.nullable<!storage.string>>>
!cells = !db.column<!storage.any>

func.func @main() {
  %a = db.scan_nodes() : !nodes
  %name = db.get_node_properties(%a, "name") : (!nodes) -> !names
  %x, %ac, %namec = db.unwind_computed({%a, %name}) {
  ^bb0(%in: !nodes, %nameIn: !names):
    %again = db.get_node_properties(%in, "name") : (!nodes) -> !names
    %names = db.make_list(%again) : (!names) -> !nameLists
    db.unwind_yield %names : !nameLists
  } : (!nodes, !names) -> (!cells, !nodes, !names)
  db.output(%x, %namec) : !cells, !names
  return
}
)mlir";

TEST_F(UnwindComputedRegionCarrySetTest, readsThePropertyItCarriesInItsRegion) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unwindReadingACarriedProperty);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));

    mlir::db::UnwindComputed unwind = theUnwind(*module);
    ASSERT_TRUE(unwind);

    EXPECT_EQ(collect<mlir::db::GetNodeProperties>(*module).size(), 1u);

    llvm::SmallVector<mlir::db::MakeList> lists = collect<mlir::db::MakeList>(*module);
    ASSERT_EQ(lists.size(), 1u);
    EXPECT_EQ(lists.front()->getOperand(0), unwind.getSource().front().getArgument(1));
}

// MATCH (a) WITH a, a.name AS name UNWIND [a.name] AS x RETURN x, a
const char* const unwindReadingAPropertyReadBefore = R"mlir(
!nodes = !db.column<!storage.node_id>
!names = !db.column<!storage.nullable<!storage.string>>
!nameLists = !db.column<!storage.list<!storage.nullable<!storage.string>>>
!cells = !db.column<!storage.any>

func.func @main() {
  %a = db.scan_nodes() : !nodes
  %name = db.get_node_properties(%a, "name") : (!nodes) -> !names
  %x, %ac = db.unwind_computed({%a}) {
  ^bb0(%in: !nodes):
    %again = db.get_node_properties(%in, "name") : (!nodes) -> !names
    %names = db.make_list(%again) : (!names) -> !nameLists
    db.unwind_yield %names : !nameLists
  } : (!nodes) -> (!cells, !nodes)
  db.output(%x, %ac) : !cells, !nodes
  return
}
)mlir";

TEST_F(UnwindComputedRegionCarrySetTest, carriesAPropertyReadBeforeItIntoItsRegion) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unwindReadingAPropertyReadBefore);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));

    mlir::db::UnwindComputed unwind = theUnwind(*module);
    ASSERT_TRUE(unwind);

    llvm::SmallVector<mlir::db::GetNodeProperties> reads = collect<mlir::db::GetNodeProperties>(*module);
    ASSERT_EQ(reads.size(), 1u);

    const mlir::OperandRange carried = unwind.getColumnsToFilter();
    ASSERT_EQ(carried.size(), 2u);
    EXPECT_EQ(carried[1], reads.front().getResult());
    EXPECT_EQ(unwind->getNumResults(), 3u);

    llvm::SmallVector<mlir::db::MakeList> lists = collect<mlir::db::MakeList>(*module);
    ASSERT_EQ(lists.size(), 1u);
    EXPECT_EQ(lists.front()->getOperand(0), unwind.getSource().front().getArgument(1));
}

// MATCH (a) WITH a, a.name AS name UNWIND [a] AS x RETURN x, a.name
const char* const readAfterTheUnwind = R"mlir(
!nodes = !db.column<!storage.node_id>
!names = !db.column<!storage.nullable<!storage.string>>
!nodeLists = !db.column<!storage.list<!storage.node_id>>
!cells = !db.column<!storage.any>

func.func @main() {
  %a = db.scan_nodes() : !nodes
  %name = db.get_node_properties(%a, "name") : (!nodes) -> !names
  %x, %ac = db.unwind_computed({%a}) {
  ^bb0(%in: !nodes):
    %xs = db.make_list(%in) : (!nodes) -> !nodeLists
    db.unwind_yield %xs : !nodeLists
  } : (!nodes) -> (!cells, !nodes)
  %later = db.get_node_properties(%ac, "name") : (!nodes) -> !names
  db.output(%x, %later) : !cells, !names
  return
}
)mlir";

TEST_F(UnwindComputedRegionCarrySetTest, handsAPropertyReadBeforeItBackToAReadAfterIt) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(readAfterTheUnwind);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));

    mlir::db::UnwindComputed unwind = theUnwind(*module);
    ASSERT_TRUE(unwind);

    llvm::SmallVector<mlir::db::GetNodeProperties> reads = collect<mlir::db::GetNodeProperties>(*module);
    ASSERT_EQ(reads.size(), 1u);

    const mlir::OperandRange carried = unwind.getColumnsToFilter();
    ASSERT_EQ(carried.size(), 2u);
    EXPECT_EQ(carried[1], reads.front().getResult());

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs.front()->getOperand(0), unwind.getElement());
    EXPECT_EQ(outputs.front()->getOperand(1), unwind.getCarried()[1]);
}
