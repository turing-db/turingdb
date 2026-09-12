#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "StorageDialect.h"

namespace {

using Conjunctions = std::vector<std::vector<std::string>>;

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

// The property the tally is narrowed to, or an empty string when it is over the scans
// themselves.
std::string propertyOf(mlir::db::CountScanRows scanRows) {
    const std::optional<llvm::StringRef> property = scanRows.getProperty();
    if (!property) {
        return {};
    }

    return std::string(property->data(), property->size());
}

void conjunctionsOf(mlir::db::CountScanRows scanRows, Conjunctions& conjunctions) {
    for (const mlir::Attribute scan : scanRows.getLabels()) {
        std::vector<std::string>& labels = conjunctions.emplace_back();
        for (const mlir::Attribute label : mlir::cast<mlir::ArrayAttr>(scan)) {
            const llvm::StringRef name = mlir::cast<mlir::StringAttr>(label).getValue();
            labels.emplace_back(name.data(), name.size());
        }
    }
}

}

// A count(*) over nothing but whole node scans is a question about how many nodes carry a
// label, which the graph answers from its own counts - so the pass replaces the count, and
// the scans it walked, with the one db.count_scan_rows that reads them.
class CountFromMetadataTest : public ::testing::Test {
protected:
    CountFromMetadataTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runCountFromMetadata(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createCountFromMetadata());

        return mlir::succeeded(passManager.run(module));
    }

    // The module holds exactly one db.count_scan_rows over the given conjunctions, and no
    // count, scan or property read is left for the engine to walk.
    void expectRewritten(mlir::ModuleOp module, const Conjunctions& expected, const std::string& property = "") {
        llvm::SmallVector<mlir::db::CountScanRows> rewritten = collect<mlir::db::CountScanRows>(module);
        ASSERT_EQ(rewritten.size(), 1u);

        Conjunctions conjunctions;
        conjunctionsOf(rewritten.front(), conjunctions);
        EXPECT_EQ(conjunctions, expected);
        EXPECT_EQ(propertyOf(rewritten.front()), property);

        EXPECT_EQ(countOps<mlir::db::Count>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::CrossProduct>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::ScanNodes>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(module), 0u);
    }

    // The count and the dataflow under it are left exactly as they were.
    void expectUntouched(mlir::ModuleOp module) {
        EXPECT_EQ(countOps<mlir::db::CountScanRows>(module), 0u);
        EXPECT_EQ(countOps<mlir::db::Count>(module), 1u);
    }

    mlir::MLIRContext _context;
};

// MATCH (a:Person) RETURN count(*)
const char* const labelScanCount = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %n = db.count(%a) rows : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a) RETURN count(*)
const char* const wholeGraphCount = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %n = db.count(%a) rows : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person) RETURN count(a) - a node ID is never null, so the non-null tally is the
// row count just as count(*) is.
const char* const labelScanCountOfNodes = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %n = db.count(%a) : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person:Founder) RETURN count(*)
const char* const multiLabelScanCount = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person", "Founder"]) : !db.column<!storage.node_id>
  %n = db.count(%a) rows : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person), (b:Interest) RETURN count(*)
const char* const crossProductCount = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  %n = db.count(%p#0) rows : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person), (b:Interest), (c) RETURN count(*) - the three factors nest as two
// products, so the conjunctions come out in pattern order.
const char* const nestedCrossProductCount = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %q:2 = db.cross_product factor {
      %b = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
      db.yield %b : !db.column<!storage.node_id>
    } factor {
      %c = db.scan_nodes() : !db.column<!storage.node_id>
      db.yield %c : !db.column<!storage.node_id>
    }
    db.yield %q#0 : !db.column<!storage.node_id>
  }
  %n = db.count(%p#0) rows : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// The count is anchored on the right factor's column; a product's row count is the same
// whichever of its columns the tally rides on.
const char* const crossProductCountOnRightFactor = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  %n = db.count(%p#1) rows : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person), (b:Interest) RETURN count(DISTINCT a) - the distinct tally charges each
// node once however many rows of the product carry it, which is not the product's size.
const char* const crossProductDistinctCount = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  %n = db.count(%p#0) distinct : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person) WHERE a.age = 32 RETURN count(*) - how many nodes hold a property value
// is not something the label counts answer.
const char* const propertyScanCount = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_property_value("age", 32 : i64, ["Person"]) : !db.column<!storage.node_id>
  %n = db.count(%a) rows : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person) WITH a LIMIT 3 RETURN count(*) - the tally is over the budgeted prefix,
// not over the scan.
const char* const limitedScanCount = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %la = db.limit(%a) count 3 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %n = db.count(%la) rows : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person), (b:Interest) WHERE b.age = 32 RETURN count(*) - one factor is a
// property scan, so the product has no size the counts can reach.
const char* const crossProductWithPropertyScan = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes_by_property_value("age", 32 : i64, ["Interest"]) : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  %n = db.count(%p#0) rows : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person) RETURN count(a.name) - the tally is over the rows where the property is
// there, so it counts the scanned nodes that hold it.
const char* const propertyCount = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %n = db.count(%name) : (!db.column<none>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a) RETURN count(a.name) - the same over an unlabelled scan.
const char* const wholeGraphPropertyCount = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %n = db.count(%name) : (!db.column<none>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person) WITH a.name AS name RETURN count(*) - a count(*) anchored on a property
// column charges every row, nulls included, so the property read is not the tally and the
// scan's row count is the answer.
const char* const propertyAnchoredWildcardCount = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %n = db.count(%name) rows : (!db.column<none>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person) RETURN count(DISTINCT a.name) - the distinct tally charges each value
// once, which is not how many nodes hold the property.
const char* const distinctPropertyCount = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %n = db.count(%name) distinct : (!db.column<none>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person), (b:Interest) RETURN count(b.name) - the op carries one property and no
// say in which of the two scans it is read from, so the product is left alone.
const char* const crossProductPropertyCount = R"mlir(
func.func @main() {
  %p:2 = db.cross_product factor {
    %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  %name = db.get_node_properties(%p#1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %n = db.count(%name) : (!db.column<none>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person)-->(b) RETURN count(b.name) - the property is read off a hop's rows, which
// the scan counts say nothing about.
const char* const hopPropertyCount = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %s, %e, %t, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %name = db.get_node_properties(%b, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %n = db.count(%name) : (!db.column<none>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

// MATCH (a:Person)-->(b) RETURN count(*) - a hop builds its own rows.
const char* const hopCount = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %s, %e, %t, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %n = db.count(%s) rows : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%n) : !db.column<none>
  return
}
)mlir";

TEST_F(CountFromMetadataTest, LabelScanCountReadsTheLabelCount) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(labelScanCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectRewritten(*module, Conjunctions {{"Person"}});
}

TEST_F(CountFromMetadataTest, WholeGraphCountReadsAnEmptyConjunction) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(wholeGraphCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectRewritten(*module, Conjunctions {{}});
}

TEST_F(CountFromMetadataTest, CountOfTheScannedNodesIsTheRowCount) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(labelScanCountOfNodes);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectRewritten(*module, Conjunctions {{"Person"}});
}

TEST_F(CountFromMetadataTest, MultiLabelScanKeepsTheWholeConjunction) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(multiLabelScanCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectRewritten(*module, Conjunctions {{"Person", "Founder"}});
}

TEST_F(CountFromMetadataTest, CrossProductCountIsOneConjunctionPerFactor) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(crossProductCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectRewritten(*module, Conjunctions {{"Person"}, {"Interest"}});
}

TEST_F(CountFromMetadataTest, NestedCrossProductCountListsEveryFactor) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(nestedCrossProductCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectRewritten(*module, Conjunctions {{"Person"}, {"Interest"}, {}});
}

TEST_F(CountFromMetadataTest, CountOnEitherFactorReadsTheSameProduct) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(crossProductCountOnRightFactor);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectRewritten(*module, Conjunctions {{"Person"}, {"Interest"}});
}

TEST_F(CountFromMetadataTest, DistinctCountIsLeftAlone) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(crossProductDistinctCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
}

TEST_F(CountFromMetadataTest, PropertyValueScanCountIsLeftAlone) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(propertyScanCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByPropertyValue>(*module), 1u);
}

TEST_F(CountFromMetadataTest, LimitedScanCountIsLeftAlone) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(limitedScanCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::Limit>(*module), 1u);
}

TEST_F(CountFromMetadataTest, OneUnreachableFactorLeavesTheProductAlone) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(crossProductWithPropertyScan);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
}

TEST_F(CountFromMetadataTest, PropertyCountReadsTheHoldersOfTheProperty) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(propertyCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectRewritten(*module, Conjunctions {{"Person"}}, "name");
}

TEST_F(CountFromMetadataTest, PropertyCountOverAnUnlabelledScanReadsAnEmptyConjunction) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(wholeGraphPropertyCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectRewritten(*module, Conjunctions {{}}, "name");
}

TEST_F(CountFromMetadataTest, WildcardCountReadsThroughAPropertyToTheScan) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(propertyAnchoredWildcardCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectRewritten(*module, Conjunctions {{"Person"}});
}

TEST_F(CountFromMetadataTest, DistinctPropertyCountIsLeftAlone) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(distinctPropertyCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 1u);
}

TEST_F(CountFromMetadataTest, PropertyCountOverAProductIsLeftAlone) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(crossProductPropertyCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
}

TEST_F(CountFromMetadataTest, PropertyCountOverAHopIsLeftAlone) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(hopPropertyCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 1u);
}

TEST_F(CountFromMetadataTest, HopCountIsLeftAlone) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(hopCount);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runCountFromMetadata(*module));

    expectUntouched(*module);
    EXPECT_EQ(countOps<mlir::db::GetOutEdges>(*module), 1u);
}
