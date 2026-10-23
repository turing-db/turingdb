#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <string>
#include <utility>

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

using LabelPair = std::pair<std::string, std::string>;

// The label of the scan an edge column's hop starts from, climbing through the crossings
// and filters that hand the column up
std::string edgeOrigin(mlir::Value column) {
    while (mlir::Operation* const crossing = column.getDefiningOp()) {
        if (mlir::db::FilterOp filter = mlir::dyn_cast<mlir::db::FilterOp>(crossing)) {
            column = filter.getColumnsToFilter()[mlir::cast<mlir::OpResult>(column).getResultNumber()];
            continue;
        } else if (!mlir::isa<mlir::db::CrossProduct, mlir::db::HashJoin>(crossing)) {
            break;
        }

        mlir::Region& leftFactor = crossing->getRegion(0);
        const size_t leftCount = leftFactor.front().getTerminator()->getNumOperands();
        const size_t resultIndex = mlir::cast<mlir::OpResult>(column).getResultNumber();
        const bool onTheLeft = resultIndex < leftCount;

        mlir::Operation* const yield = crossing->getRegion(onTheLeft ? 0 : 1).front().getTerminator();
        column = yield->getOperand(onTheLeft ? resultIndex : resultIndex - leftCount);
    }

    mlir::db::GetOutEdges hop = column.getDefiningOp<mlir::db::GetOutEdges>();
    if (!hop) {
        return "?";
    }

    mlir::db::ScanNodesByLabel scan = hop.getInputNodes().getDefiningOp<mlir::db::ScanNodesByLabel>();
    if (!scan) {
        return "?";
    }

    const mlir::ArrayAttr labels = scan.getLabels();

    return mlir::cast<mlir::StringAttr>(labels[0]).getValue().str();
}

// Every pair the crossings of the module leave out, named by the scans its two edges come
// from
void collectLeftOutPairs(mlir::ModuleOp module, std::set<LabelPair>& pairs) {
    module.walk([&pairs](mlir::Operation* crossing) {
        mlir::DenseI64ArrayAttr held;
        if (mlir::db::CrossProduct product = mlir::dyn_cast<mlir::db::CrossProduct>(crossing)) {
            held = product.getDistinctPairsAttr();
        } else if (mlir::db::HashJoin join = mlir::dyn_cast<mlir::db::HashJoin>(crossing)) {
            held = join.getDistinctPairsAttr();
        }

        if (!held) {
            return;
        }

        mlir::Operation* const leftYield = crossing->getRegion(0).front().getTerminator();
        mlir::Operation* const rightYield = crossing->getRegion(1).front().getTerminator();
        const llvm::ArrayRef<int64_t> indices = held.asArrayRef();
        for (size_t position = 0; position + 1 < indices.size(); position += 2) {
            std::string left = edgeOrigin(leftYield->getOperand(indices[position]));
            std::string right = edgeOrigin(rightYield->getOperand(indices[position + 1]));
            if (right < left) {
                std::swap(left, right);
            }

            pairs.emplace(left, right);
        }
    });
}

// A and B crossed with an edge column each, nothing reading B's node: the trim drops it, and
// with it the place of B's edge in its yield
const char* const productProgram = R"mlir(
func.func @main() {
  %p:6 = db.cross_product factor {
    %n = db.scan_nodes_by_label(["A"]) : !db.column<!storage.node_id>
    %s, %e, %t, %d = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %s, %d, %e : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  } factor {
    %n = db.scan_nodes_by_label(["B"]) : !db.column<!storage.node_id>
    %s, %e, %t, %d = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %s, %d, %e : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  } distinct_pairs [2, 2]
  db.output(%p#0, %p#4) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const joinProgram = R"mlir(
func.func @main() {
  %p:6 = db.hash_join factor {
    %n = db.scan_nodes_by_label(["A"]) : !db.column<!storage.node_id>
    %s, %e, %t, %d = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %s, %d, %e : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  } factor {
    %n = db.scan_nodes_by_label(["B"]) : !db.column<!storage.node_id>
    %s, %e, %t, %d = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %s, %d, %e : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  } on 1, 1 distinct_pairs [2, 2]
  db.output(%p#0, %p#3) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The ends of A's and B's edges held equal over the product: the join takes the product's
// place, building the factor without a product of its own
const char* const equalityOverProductProgram = R"mlir(
func.func @main() {
  %p:9 = db.cross_product factor {
    %n = db.scan_nodes_by_label(["A"]) : !db.column<!storage.node_id>
    %s, %e, %t, %d = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %s, %d, %e : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  } factor {
    %q:6 = db.cross_product factor {
      %n = db.scan_nodes_by_label(["B"]) : !db.column<!storage.node_id>
      %s, %e, %t, %d = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
      db.yield %s, %d, %e : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
    } factor {
      %n = db.scan_nodes_by_label(["C"]) : !db.column<!storage.node_id>
      %s, %e, %t, %d = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
      db.yield %s, %d, %e : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
    } distinct_pairs [2, 2]
    db.yield %q#0, %q#1, %q#2, %q#3, %q#4, %q#5 : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  } distinct_pairs [2, 2, 2, 5]
  %eq = db.eq %p#1, %p#4 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %f:3 = db.filter(%eq, {%p#0, %p#3, %p#6}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%f#0, %f#1, %f#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// (A x B) x C with B's end held equal to C's: the cut rotates the product so that B and C
// are crossed first, which moves the pairs of the old nest onto the new one
const char* const rotatedProductProgram = R"mlir(
func.func @main() {
  %p:9 = db.cross_product factor {
    %q:6 = db.cross_product factor {
      %n = db.scan_nodes_by_label(["A"]) : !db.column<!storage.node_id>
      %s, %e, %t, %d = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
      db.yield %s, %d, %e : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
    } factor {
      %n = db.scan_nodes_by_label(["B"]) : !db.column<!storage.node_id>
      %s, %e, %t, %d = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
      db.yield %s, %d, %e : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
    } distinct_pairs [2, 2]
    db.yield %q#0, %q#1, %q#2, %q#3, %q#4, %q#5 : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  } factor {
    %n = db.scan_nodes_by_label(["C"]) : !db.column<!storage.node_id>
    %s, %e, %t, %d = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %s, %d, %e : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  } distinct_pairs [2, 2, 5, 2]
  %eq = db.eq %p#4, %p#7 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %f:3 = db.filter(%eq, {%p#0, %p#3, %p#6}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%f#0, %f#1, %f#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

// A rewrite rebuilding a cross product or a hash join keeps the pairs of edges it leaves
// out, renumbered to where their columns land, whatever the pass order
class DistinctPairsRewriteTest : public ::testing::Test {
protected:
    DistinctPairsRewriteTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    void runPass(mlir::ModuleOp module, std::unique_ptr<mlir::Pass> pass) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(std::move(pass));

        ASSERT_TRUE(mlir::succeeded(passManager.run(module)));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(module)));
    }

    void expectPairs(mlir::ModuleOp module, const std::set<LabelPair>& expected) {
        std::set<LabelPair> pairs;
        collectLeftOutPairs(module, pairs);

        EXPECT_EQ(pairs, expected);
    }

    mlir::MLIRContext _context;
    const mlir::db::DBPassContext _passContext;
};

TEST_F(DistinctPairsRewriteTest, keepsThePairThroughATrimmedProduct) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(productProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createTrimUnreadColumns());

    llvm::SmallVector<mlir::db::CrossProduct> products;
    module->walk([&products](mlir::db::CrossProduct product) { products.push_back(product); });
    ASSERT_EQ(products.size(), 1u);
    EXPECT_EQ(products.front()->getNumResults(), 4u);

    expectPairs(*module, {{"A", "B"}});
}

TEST_F(DistinctPairsRewriteTest, keepsThePairThroughATrimmedJoin) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(joinProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createTrimUnreadColumns());

    expectPairs(*module, {{"A", "B"}});
}

TEST_F(DistinctPairsRewriteTest, keepsThePairsThroughTheJoinTakingTheProductsPlace) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(equalityOverProductProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createFuseHashJoin(&_passContext));

    llvm::SmallVector<mlir::db::HashJoin> joins;
    module->walk([&joins](mlir::db::HashJoin join) { joins.push_back(join); });
    ASSERT_EQ(joins.size(), 1u);

    expectPairs(*module, {{"A", "B"}, {"A", "C"}, {"B", "C"}});
}

TEST_F(DistinctPairsRewriteTest, keepsThePairsThroughARotatedProduct) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(rotatedProductProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createPlaceInFactors());

    expectPairs(*module, {{"A", "B"}, {"A", "C"}, {"B", "C"}});
}
