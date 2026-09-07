#include <gtest/gtest.h>

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
#include "StorageDialect.h"

namespace {

template <typename OpType>
llvm::SmallVector<OpType> collect(mlir::ModuleOp module) {
    llvm::SmallVector<OpType> ops;
    module.walk([&](OpType op) {
        ops.push_back(op);
    });

    return ops;
}

template <typename OpType>
size_t countOps(mlir::ModuleOp module) {
    return collect<OpType>(module).size();
}

}

class ReusePropertyReadsTest : public ::testing::Test {
protected:
    ReusePropertyReadsTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runReuse(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createReusePropertyReads());

        return mlir::succeeded(passManager.run(module));
    }

    // The production order: the reuse widens the carry sets, then the trim cuts back the
    // columns the collapsed reads left behind.
    bool runReuseThenTrim(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createReusePropertyReads());
        passManager.addPass(mlir::db::createTrimUnreadColumns());

        return mlir::succeeded(passManager.run(module));
    }

    mlir::MLIRContext _context;
};

// MATCH (n) RETURN n.age, n.age + 1
const char* const twoReadsOfOneColumn = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %again = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %one = db.constant(1 : i64)
  %sum = db.add %again, %one : (!db.column<none>, !db.column<i64>) -> !db.column<none>
  db.output(%age, %sum) : !db.column<none>, !db.column<none>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, collapsesTwoReadsOfTheSameColumn) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoReadsOfOneColumn);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetNodeProperties> reads = collect<mlir::db::GetNodeProperties>(*module);
    ASSERT_EQ(reads.size(), 1u);

    llvm::SmallVector<mlir::db::AddOp> sums = collect<mlir::db::AddOp>(*module);
    ASSERT_EQ(sums.size(), 1u);
    EXPECT_EQ(sums.front().getLhs(), reads.front().getResult());

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    ASSERT_EQ(outputs.front().getColumns().size(), 2u);
    EXPECT_EQ(outputs.front().getColumns()[0], reads.front().getResult());
}

// A read of another property of the same column has nothing to share.
const char* const twoPropertiesOfOneColumn = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%age, %name) : !db.column<none>, !db.column<none>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, keepsTheReadsOfTwoProperties) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoPropertiesOfOneColumn);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 2u);
}

// MATCH (n) WHERE n.age > 42 AND n.age < 50 RETURN n, as codegen splits it: one filter per
// conjunct, and the second reads the age of the rows the first kept.
const char* const splitRangeFilter = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %low = db.constant(42 : i64)
  %above = db.gt %age, %low : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %kept = db.filter(%above, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %keptAge = db.get_node_properties(%kept, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %high = db.constant(50 : i64)
  %below = db.lt %keptAge, %high : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %left = db.filter(%below, {%kept}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%left) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, carriesTheAgeThroughTheFilterOfARange) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(splitRangeFilter);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetNodeProperties> reads = collect<mlir::db::GetNodeProperties>(*module);
    ASSERT_EQ(reads.size(), 1u);

    llvm::SmallVector<mlir::db::ScanNodes> scans = collect<mlir::db::ScanNodes>(*module);
    ASSERT_EQ(scans.size(), 1u);
    EXPECT_EQ(reads.front().getInputNodes(), scans.front().getResult());

    // The first filter now carries the age beside the nodes, and the second predicate reads
    // that carried column.
    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 2u);
    mlir::db::FilterOp firstFilter = filters[0];
    ASSERT_EQ(firstFilter.getColumnsToFilter().size(), 2u);
    EXPECT_EQ(firstFilter.getColumnsToFilter()[1], reads.front().getResult());

    llvm::SmallVector<mlir::db::LtOp> comparisons = collect<mlir::db::LtOp>(*module);
    ASSERT_EQ(comparisons.size(), 1u);
    EXPECT_EQ(comparisons.front().getLhs(), firstFilter.getResult(1));
}

// MATCH (n) WHERE n.age > 42 AND n.age < 50 RETURN n.age: the projection reads the age a
// third time, two filters down from the read that stays.
const char* const splitRangeFilterReturningAge = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %low = db.constant(42 : i64)
  %above = db.gt %age, %low : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %kept = db.filter(%above, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %keptAge = db.get_node_properties(%kept, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %high = db.constant(50 : i64)
  %below = db.lt %keptAge, %high : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %left = db.filter(%below, {%kept}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %leftAge = db.get_node_properties(%left, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%leftAge) : !db.column<none>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, carriesTheAgeDownToTheProjection) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(splitRangeFilterReturningAge);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    ASSERT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 1u);

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 2u);
    mlir::db::FilterOp secondFilter = filters[1];
    ASSERT_EQ(secondFilter.getColumnsToFilter().size(), 2u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    ASSERT_EQ(outputs.front().getColumns().size(), 1u);
    EXPECT_EQ(outputs.front().getColumns().front(), secondFilter.getResult(1));
}

// With nothing left reading the node column, the trim drops it from the second filter and
// the projection rides the carried age alone.
TEST_F(ReusePropertyReadsTest, leavesTheTrimAFilterOfOneColumn) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(splitRangeFilterReturningAge);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuseThenTrim(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    ASSERT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 1u);

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 2u);
    EXPECT_EQ(filters[1].getColumnsToFilter().size(), 1u);
}

// MATCH (a)-->(b) WHERE a.age > 42 RETURN a.age: the hop stands between the two reads.
const char* const readAcrossAHop = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%a, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %low = db.constant(42 : i64)
  %above = db.gt %age, %low : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %kept = db.filter(%above, {%a}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%kept, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %hoppedAge = db.get_node_properties(%s, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%hoppedAge, %t) : !db.column<none>, !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, carriesTheAgeThroughAHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(readAcrossAHop);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    ASSERT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 1u);

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    mlir::db::GetOutEdges hop = hops.front();
    ASSERT_EQ(hop.getColumnsToFilter().size(), 1u);
    ASSERT_EQ(hop.getFilteredColumns().size(), 1u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs.front().getColumns()[0], hop.getFilteredColumns()[0]);
}

// A reverse hop hands the input node column back as tgtids, not as srcids.
const char* const readAcrossAReverseHop = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%a, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %s, %e, %et, %t = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %hoppedAge = db.get_node_properties(%t, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%age, %hoppedAge) : !db.column<none>, !db.column<none>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, carriesTheAgeThroughAReverseHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(readAcrossAReverseHop);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    ASSERT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 1u);

    llvm::SmallVector<mlir::db::GetInEdges> hops = collect<mlir::db::GetInEdges>(*module);
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_EQ(hops.front().getFilteredColumns().size(), 1u);
}

// The node a forward hop reaches is not the node it started from, so its property is a read
// of its own.
const char* const readOfTheHopTarget = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%a, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %targetAge = db.get_node_properties(%t, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%age, %targetAge) : !db.column<none>, !db.column<none>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, keepsTheReadOfAHopTarget) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(readOfTheHopTarget);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 2u);
    EXPECT_EQ(collect<mlir::db::GetOutEdges>(*module).front().getColumnsToFilter().size(), 0u);
}

// MATCH (n) WITH n ORDER BY n.age SKIP 1 LIMIT 2 RETURN n.age
const char* const readAcrossASortAndCuts = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %sn, %sage = db.sort(%n, %age) keys [1] ascending [true] : (!db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
  %kn = db.skip(%sn) count 1 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %ln = db.limit(%kn) count 2 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %cutAge = db.get_node_properties(%ln, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%cutAge) : !db.column<none>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, carriesTheSortKeyThroughTheCuts) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(readAcrossASortAndCuts);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    ASSERT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 1u);

    llvm::SmallVector<mlir::db::Skip> skips = collect<mlir::db::Skip>(*module);
    ASSERT_EQ(skips.size(), 1u);
    EXPECT_EQ(skips.front().getColumns().size(), 2u);

    llvm::SmallVector<mlir::db::Limit> limits = collect<mlir::db::Limit>(*module);
    ASSERT_EQ(limits.size(), 1u);
    ASSERT_EQ(limits.front().getColumns().size(), 2u);

    // The sort still orders by the age it already held, at the index it was given.
    llvm::SmallVector<mlir::db::Sort> sorts = collect<mlir::db::Sort>(*module);
    ASSERT_EQ(sorts.size(), 1u);
    ASSERT_EQ(sorts.front().getKeyColumns().size(), 1u);
    EXPECT_EQ(sorts.front().getKeyColumns()[0], 1);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs.front().getColumns().front(), limits.front().getResult(1));
}

// Every column of a remove_duplicates is part of its dedup key, so carrying the age through
// it would change which rows survive: the second read stays.
const char* const readAcrossRemoveDuplicates = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %dn = db.remove_duplicates(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %dage = db.get_node_properties(%dn, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%age, %dage) : !db.column<none>, !db.column<none>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, keepsTheReadPastARemoveDuplicates) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(readAcrossRemoveDuplicates);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 2u);
    EXPECT_EQ(collect<mlir::db::RemoveDuplicates>(*module).front().getColumns().size(), 1u);
}

// A group's rows are not the rows it grouped, so the age of a key is not the age of the
// nodes that fell into it.
const char* const readAcrossAGroupAggregate = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gn, %gc = db.group_aggregate(%n, %n) keys 1 aggregates [count] : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<ui64>)
  %gage = db.get_node_properties(%gn, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%age, %gage, %gc) : !db.column<none>, !db.column<none>, !db.column<ui64>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, keepsTheReadPastAGroupAggregate) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(readAcrossAGroupAggregate);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 2u);
    EXPECT_EQ(collect<mlir::db::GroupAggregate>(*module).front().getColumns().size(), 2u);
}

// A read standing after the filter it would have to be carried through cannot be the one
// that is kept.
const char* const readAfterTheFilter = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%n, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %who = db.constant("Remy" : !storage.string)
  %mask = db.neq %name, %who : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %kept = db.filter(%mask, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %keptAge = db.get_node_properties(%kept, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%age, %keptAge) : !db.column<none>, !db.column<none>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, keepsAReadStandingAfterTheFilter) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(readAfterTheFilter);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 3u);
    EXPECT_EQ(collect<mlir::db::FilterOp>(*module).front().getColumnsToFilter().size(), 1u);
}

// The edge read of an edge property is the counterpart of the node one, and a node read
// never stands in for it.
const char* const edgePropertyAcrossAFilter = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %weight = db.get_edge_properties(%e, "weight") : (!db.column<!storage.edge_id>) -> !db.column<none>
  %low = db.constant(1 : i64)
  %above = db.gt %weight, %low : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %kept = db.filter(%above, {%e}) : (!db.column<!storage.bool>, !db.column<!storage.edge_id>) -> !db.column<!storage.edge_id>
  %keptWeight = db.get_edge_properties(%kept, "weight") : (!db.column<!storage.edge_id>) -> !db.column<none>
  db.output(%keptWeight) : !db.column<none>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, carriesAnEdgePropertyThroughAFilter) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(edgePropertyAcrossAFilter);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetEdgeProperties> reads = collect<mlir::db::GetEdgeProperties>(*module);
    ASSERT_EQ(reads.size(), 1u);

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);
    ASSERT_EQ(filters.front().getColumnsToFilter().size(), 2u);
    EXPECT_EQ(filters.front().getColumnsToFilter()[1], reads.front().getResult());

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs.front().getColumns().front(), filters.front().getResult(1));
}

// A carry set already holding the column is reused rather than widened again.
const char* const twoReadsPastOneCarriedAge = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %low = db.constant(42 : i64)
  %above = db.gt %age, %low : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %kn, %kage = db.filter(%above, {%n, %age}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
  %readAgain = db.get_node_properties(%kn, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%kage, %readAgain) : !db.column<none>, !db.column<none>
  return
}
)mlir";

TEST_F(ReusePropertyReadsTest, reusesAColumnTheCarrySetAlreadyHolds) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoReadsPastOneCarriedAge);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    ASSERT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 1u);

    llvm::SmallVector<mlir::db::FilterOp> filters = collect<mlir::db::FilterOp>(*module);
    ASSERT_EQ(filters.size(), 1u);
    EXPECT_EQ(filters.front().getColumnsToFilter().size(), 2u);

    llvm::SmallVector<mlir::db::Output> outputs = collect<mlir::db::Output>(*module);
    ASSERT_EQ(outputs.size(), 1u);
    EXPECT_EQ(outputs.front().getColumns()[0], filters.front().getResult(1));
    EXPECT_EQ(outputs.front().getColumns()[1], filters.front().getResult(1));
}
