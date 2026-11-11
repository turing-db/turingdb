#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OperationSupport.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "IRRaiseEffect.h"
#include "StorageDialect.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view BODY_MARKER = "@BODY@";

constexpr std::string_view RAISING_DIVISION = R"mlir(%three = db.constant(3 : i64)
    %divisor = db.constant(0 : i64)
    %q = db.div %three, %divisor : (!i64s, !i64s) -> !any)mlir";

constexpr std::string_view SAFE_DIVISION = R"mlir(%three = db.constant(3 : i64)
    %divisor = db.constant(3 : i64)
    %q = db.div %three, %divisor : (!i64s, !i64s) -> !any)mlir";

// An op holding a body, named as it prints, and the ops making it, the body marker in its body
struct RegionOpProgram {
    std::string_view _opName;
    std::string_view _ops;
};

constexpr RegionOpProgram UNWIND_COMPUTED {"db.unwind_computed", R"mlir(
  %x, %carried = db.unwind_computed({%rows}) {
  ^bb0(%in: !cells):
    @BODY@
    %xs = db.make_list(%in) : (!cells) -> !db.column<!storage.list<!storage.any>>
    db.unwind_yield %xs : !db.column<!storage.list<!storage.any>>
  } : (!cells) -> (!cells, !cells)
)mlir"};

constexpr RegionOpProgram LIST_COMPREHENSION {"db.list_comprehension", R"mlir(
  %xs = db.constant([1, 2])
  %l = db.list_comprehension(%xs, {%rows}) {
  ^bb0(%x: !any, %tag: !tags, %row: !cells):
    @BODY@
    db.comprehension_yield %tag, %row : !cells
  } : (!lists, !cells) -> !any
)mlir"};

constexpr RegionOpProgram LIST_PREDICATE {"db.list_predicate", R"mlir(
  %xs = db.constant([1, 2])
  %p = db.list_predicate(%xs, {%rows}) kind any {
  ^bb0(%x: !any, %tag: !tags, %row: !cells):
    @BODY@
    %held = db.is_not_null %row : (!cells) -> !bools
    db.comprehension_yield %tag, %held : !bools
  } : (!lists, !cells) -> !bools
)mlir"};

constexpr RegionOpProgram REDUCE {"db.reduce", R"mlir(
  %xs = db.constant([1, 2])
  %r = db.reduce(%xs, %rows, {}) {
  ^bb0(%x: !any, %acc: !any):
    @BODY@
    db.reduce_yield %acc : !any
  } : (!lists, !cells) -> !any
)mlir"};

constexpr RegionOpProgram ALL_REDUCE {"db.all_reduce", R"mlir(
  %xs = db.constant([1, 2])
  %r = db.all_reduce(%xs, %rows, {}) {
  ^bb0(%x: !any, %acc: !any):
    db.reduce_yield %acc : !any
  } predicate {
  ^bb0(%x: !any, %acc: !any):
    @BODY@
    %held = db.is_not_null %acc : (!any) -> !bools
    db.all_reduce_yield %held : !bools
  } : (!lists, !cells) -> !bools
)mlir"};

constexpr RegionOpProgram PATTERN_COMPREHENSION {"db.pattern_comprehension", R"mlir(
  %l = db.pattern_comprehension(%n, %rows) {
  ^bb0(%nIn: !nodes, %row: !cells, %tag: !tags):
    @BODY@
    db.comprehension_yield %tag, %row : !cells
  } : (!nodes, !cells) -> !any
)mlir"};

constexpr RegionOpProgram EXPLORE_PATHS {"db.explore_paths", R"mlir(
  %paths:3 = db.explore_paths(%n, {}) forward hops 1 to 2 {
  ^bb0(%source: !nodes, %edge: !edges, %end: !nodes):
    @BODY@
    %held = db.is_not_null %end : (!nodes) -> !bools
    db.yield %held : !bools
  } : (!nodes) -> (!nodes, !nodes, !db.column<!storage.path_ref>)
)mlir"};

constexpr RegionOpProgram CROSS_PRODUCT {"db.cross_product", R"mlir(
  %pairs:2 = db.cross_product factor {
    %y = db.scan_nodes() : !nodes
    @BODY@
    db.yield %y : !nodes
  } factor {
    %z = db.scan_nodes() : !nodes
    db.yield %z : !nodes
  }
)mlir"};

constexpr RegionOpProgram HASH_JOIN {"db.hash_join", R"mlir(
  %joined:2 = db.hash_join factor {
    %y = db.scan_nodes() : !nodes
    db.yield %y : !nodes
  } factor {
    %z = db.scan_nodes() : !nodes
    @BODY@
    db.yield %z : !nodes
  } on 0, 0
)mlir"};

constexpr RegionOpProgram LAZY_CASE {"db.lazy_case", R"mlir(
  %c = db.lazy_case({%rows}) {
  ^bb0(%x: !cells):
    %held = db.is_not_null %x : (!cells) -> !bools
    db.case_yield %held : !bools
  }, {
  ^bb0(%x: !cells):
    @BODY@
    db.case_yield %q : !any
  } : (!cells) -> !any
)mlir"};

constexpr RegionOpProgram COALESCE {"db.coalesce", R"mlir(
  %c = db.coalesce({%rows}) {
  ^bb0(%x: !cells):
    db.case_yield %x : !cells
  }, {
  ^bb0(%x: !cells):
    @BODY@
    db.case_yield %q : !any
  } : (!cells) -> !any
)mlir"};

constexpr RegionOpProgram OPTIONAL_MATCH {"db.optional_match", R"mlir(
  %matched:2 = db.optional_match(%rows) {
  ^bb0(%in: !cells, %tag: !tags):
    @BODY@
    %pairs:3 = db.cross_product factor {
      db.yield %in, %tag : !cells, !tags
    } factor {
      %m = db.scan_nodes() : !nodes
      db.yield %m : !nodes
    }
    db.optional_yield %pairs#1, {%pairs#0, %pairs#2} : {!cells, !nodes}
  } : (!cells) -> (!cells, !nodes)
)mlir"};

constexpr RegionOpProgram EXISTS_SUBQUERY {"db.exists_subquery", R"mlir(
  %b = db.exists_subquery(%n) carries_scope {
  ^bb0(%nIn: !nodes, %tag: !tags):
    @BODY@
    db.exists_yield %tag, {}
  } : (!nodes) -> !bools
)mlir"};

constexpr RegionOpProgram COUNT_SUBQUERY {"db.count_subquery", R"mlir(
  %c = db.count_subquery(%n) {
  ^bb0(%nIn: !nodes):
    @BODY@
    %row = db.row_constant %nIn : !nodes
    db.count_subquery_yield {%row} : {!nodes}
  } : (!nodes) -> !tags
)mlir"};

constexpr RegionOpProgram COLLECT_SUBQUERY {"db.collect_subquery", R"mlir(
  %l = db.collect_subquery(%rows) carries_scope {
  ^bb0(%row: !cells, %tag: !tags):
    @BODY@
    db.collect_subquery_yield tag %tag, %row, {} : !cells
  } : (!cells) -> !any
)mlir"};

constexpr RegionOpProgram REGION_OPS[] = {UNWIND_COMPUTED, LIST_COMPREHENSION, LIST_PREDICATE, REDUCE,
                                          ALL_REDUCE, PATTERN_COMPREHENSION, EXPLORE_PATHS, CROSS_PRODUCT,
                                          HASH_JOIN, LAZY_CASE, COALESCE, OPTIONAL_MATCH, EXISTS_SUBQUERY,
                                          COUNT_SUBQUERY, COLLECT_SUBQUERY};

// The ones computing a value for each row they read, which the unread-value erasers try
constexpr RegionOpProgram VALUE_REGION_OPS[] = {LIST_COMPREHENSION, LIST_PREDICATE, REDUCE, ALL_REDUCE,
                                                PATTERN_COMPREHENSION, LAZY_CASE, COALESCE, EXISTS_SUBQUERY,
                                                COUNT_SUBQUERY, COLLECT_SUBQUERY};

std::string_view nameOf(mlir::Operation* op) {
    const llvm::StringRef name = op->getName().getStringRef();
    return std::string_view(name.data(), name.size());
}

}

// An op holding a body raises where an op its body holds may, however deep, so one nothing
// reads stays where its body divides by zero, and goes where its body cannot raise. Each
// program divides 3 by a constant 0 in the raising form and by a constant 3 in the safe one.
class RegionBodyEffectsTest : public ::testing::Test {
protected:
    RegionBodyEffectsTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    // Parses @param ops, @param division in place of the body marker, into a function taking
    // %n, a column of nodes, %rows, of cells, %map, of maps, and %path, of paths
    bool parse(std::string_view ops, std::string_view division) {
        std::string body(ops);
        const size_t marker = body.find(BODY_MARKER);
        if (marker != std::string::npos) {
            body.replace(marker, BODY_MARKER.size(), division);
        }

        std::string program = R"mlir(
!nodes = !db.column<!storage.node_id>
!edges = !db.column<!storage.edge_id>
!cells = !db.column<!storage.any>
!maps = !db.column<!storage.map>
!paths = !db.column<!storage.path>
!strings = !db.column<!storage.string>
!any = !db.column<none>
!bools = !db.column<!storage.bool>
!i64s = !db.column<i64>
!tags = !db.column<ui64>
!lists = !db.column<!storage.list<i64>>

func.func @main(%n: !nodes, %rows: !cells, %map: !maps, %path: !paths) {
)mlir";

        program += body;
        program += "\n  return\n}\n";

        _module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        EXPECT_TRUE(_module) << program;

        return static_cast<bool>(_module);
    }

    mlir::Operation* lastOp() {
        mlir::func::FuncOp main = _module->lookupSymbol<mlir::func::FuncOp>("main");
        return main.getBody().front().getTerminator()->getPrevNode();
    }

    template <typename CreatePass>
    bool runPass(CreatePass createPass) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(createPass());

        const bool ran = mlir::succeeded(passManager.run(*_module)) && mlir::succeeded(mlir::verify(*_module));
        EXPECT_TRUE(ran);

        return ran;
    }

    size_t countOps(std::string_view opName) {
        size_t count = 0;
        _module->walk([opName, &count](mlir::Operation* op) {
            if (nameOf(op) == opName) {
                count++;
            }
        });

        return count;
    }

    // The ops named @param opName that @param createPass leaves of @param ops, @param division
    // in its body
    template <typename CreatePass>
    size_t countAfterPass(std::string_view ops, std::string_view opName, std::string_view division, CreatePass createPass) {
        if (!parse(ops, division) || !runPass(createPass)) {
            return SIZE_MAX;
        }

        return countOps(opName);
    }

    mlir::MLIRContext _context;
    mlir::OwningOpRef<mlir::ModuleOp> _module;
};

TEST_F(RegionBodyEffectsTest, keepsAnUnreadOpWhoseBodyMayRaise) {
    for (const RegionOpProgram& regionOp : REGION_OPS) {
        ASSERT_TRUE(parse(regionOp._ops, RAISING_DIVISION));
        mlir::Operation* const op = lastOp();
        ASSERT_EQ(nameOf(op), regionOp._opName);

        EXPECT_FALSE(mlir::isOpTriviallyDead(op)) << regionOp._opName;
        EXPECT_FALSE(mlir::wouldOpBeTriviallyDead(op)) << regionOp._opName;
        EXPECT_FALSE(mlir::isSpeculatable(op)) << regionOp._opName;
        EXPECT_FALSE(mlir::isPure(op)) << regionOp._opName;
        EXPECT_TRUE(::db::hasNoEffectButRaising(op)) << regionOp._opName;
    }
}

TEST_F(RegionBodyEffectsTest, dropsAnUnreadOpWhoseBodyCannotRaise) {
    for (const RegionOpProgram& regionOp : REGION_OPS) {
        ASSERT_TRUE(parse(regionOp._ops, SAFE_DIVISION));
        mlir::Operation* const op = lastOp();
        ASSERT_EQ(nameOf(op), regionOp._opName);

        EXPECT_TRUE(mlir::isOpTriviallyDead(op)) << regionOp._opName;
        EXPECT_TRUE(mlir::isSpeculatable(op)) << regionOp._opName;
        EXPECT_TRUE(mlir::isPure(op)) << regionOp._opName;
        EXPECT_TRUE(::db::hasNoEffectButRaising(op)) << regionOp._opName;
    }
}

// A body nested in a body raises through both
TEST_F(RegionBodyEffectsTest, keepsAnUnreadOpWhoseNestedBodyMayRaise) {
    const std::string_view nested = R"mlir(
  %xs = db.constant([1, 2])
  %l = db.list_comprehension(%xs, {%n}) {
  ^bb0(%x: !any, %tag: !tags, %nIn: !nodes):
    %c = db.count_subquery(%nIn) {
    ^bb0(%m: !nodes):
      @BODY@
      %row = db.row_constant %m : !nodes
      db.count_subquery_yield {%row} : {!nodes}
    } : (!nodes) -> !tags
    db.comprehension_yield %tag, %c : !tags
  } : (!lists, !nodes) -> !any
)mlir";

    ASSERT_TRUE(parse(nested, RAISING_DIVISION));
    EXPECT_FALSE(mlir::isOpTriviallyDead(lastOp()));
    EXPECT_FALSE(mlir::isSpeculatable(lastOp()));

    ASSERT_TRUE(parse(nested, SAFE_DIVISION));
    EXPECT_TRUE(mlir::isOpTriviallyDead(lastOp()));
    EXPECT_TRUE(mlir::isSpeculatable(lastOp()));
}

// A cell holding no node, or no path, is the type error the row raises
TEST_F(RegionBodyEffectsTest, keepsAnUnreadReadOfAnEntityOrAPathOffACell) {
    ASSERT_TRUE(parse("%r = db.checked_cast %rows : (!cells) -> !nodes", ""));
    EXPECT_FALSE(mlir::isOpTriviallyDead(lastOp()));

    ASSERT_TRUE(parse("%r = db.checked_cast %n : (!nodes) -> !nodes", ""));
    EXPECT_TRUE(mlir::isOpTriviallyDead(lastOp()));

    ASSERT_TRUE(parse("%r = db.path_elements(%rows) kind length : (!cells) -> !db.column<!storage.nullable<ui64>>", ""));
    EXPECT_FALSE(mlir::isOpTriviallyDead(lastOp()));

    ASSERT_TRUE(parse("%r = db.path_elements(%path) kind length : (!paths) -> !db.column<!storage.nullable<ui64>>", ""));
    EXPECT_TRUE(mlir::isOpTriviallyDead(lastOp()));
}

// An op reporting no effect cannot raise, and an op holding a body reports the raise it holds
TEST_F(RegionBodyEffectsTest, everyOpReportingItsEffectsReportsItsRaise) {
    for (const mlir::RegisteredOperationName name : _context.getRegisteredOperations()) {
        const bool reportsItsEffects = name.hasInterface<mlir::MemoryEffectOpInterface>();
        if (name.getDialectNamespace() != "db" || !reportsItsEffects) {
            continue;
        }

        const bool reportsARaise = name.hasTrait<mlir::OpTrait::MayRaise>();
        const bool neverRaises = name.hasTrait<mlir::OpTrait::AlwaysSpeculatableImplTrait>();
        EXPECT_TRUE(reportsARaise || neverRaises) << name.getStringRef().str();

        if (!name.hasTrait<mlir::OpTrait::ZeroRegions>()) {
            const bool raisesThroughItsBody = name.hasTrait<mlir::OpTrait::RecursivelySpeculatableImplTrait>();
            EXPECT_TRUE(reportsARaise && raisesThroughItsBody) << name.getStringRef().str();
        }
    }
}

TEST_F(RegionBodyEffectsTest, eraseUnreadValuesKeepsABodyThatMayRaise) {
    for (const RegionOpProgram& regionOp : VALUE_REGION_OPS) {
        EXPECT_EQ(countAfterPass(regionOp._ops, regionOp._opName, RAISING_DIVISION, mlir::db::createEraseUnreadOps), 1u)
            << regionOp._opName;
        EXPECT_EQ(countAfterPass(regionOp._ops, regionOp._opName, SAFE_DIVISION, mlir::db::createEraseUnreadOps), 0u)
            << regionOp._opName;
    }
}

TEST_F(RegionBodyEffectsTest, trimUnreadColumnsKeepsABodyThatMayRaise) {
    for (const RegionOpProgram& regionOp : VALUE_REGION_OPS) {
        EXPECT_EQ(countAfterPass(regionOp._ops, regionOp._opName, RAISING_DIVISION, mlir::db::createTrimUnreadColumns), 1u)
            << regionOp._opName;
        EXPECT_EQ(countAfterPass(regionOp._ops, regionOp._opName, SAFE_DIVISION, mlir::db::createTrimUnreadColumns), 0u)
            << regionOp._opName;
    }
}

// The pattern of an optional match drops a column nothing reads past it, and the ops only it
// read go with it
TEST_F(RegionBodyEffectsTest, trimUnreadColumnsKeepsABodyThatMayRaiseInAPattern) {
    const std::string_view optional = R"mlir(
  %matched:3 = db.optional_match(%rows) {
  ^bb0(%in: !cells, %tag: !tags):
    %pairs:3 = db.cross_product factor {
      db.yield %in, %tag : !cells, !tags
    } factor {
      %m = db.scan_nodes() : !nodes
      db.yield %m : !nodes
    }
    %xs = db.constant([1, 2])
    %l = db.list_comprehension(%xs, {%pairs#0}) {
    ^bb0(%x: !any, %elementTag: !tags, %row: !cells):
      @BODY@
      db.comprehension_yield %elementTag, %row : !cells
    } : (!lists, !cells) -> !any
    db.optional_yield %pairs#1, {%pairs#0, %pairs#2, %l} : {!cells, !nodes, !any}
  } : (!cells) -> (!cells, !nodes, !any)
  db.output(%matched#1) : !nodes
)mlir";

    EXPECT_EQ(countAfterPass(optional, "db.list_comprehension", RAISING_DIVISION, mlir::db::createTrimUnreadColumns), 1u);
    EXPECT_EQ(countAfterPass(optional, "db.list_comprehension", SAFE_DIVISION, mlir::db::createTrimUnreadColumns), 0u);
}

// The key read off a cross product, or off the walk carrying it, folds to the key it holds,
// and leaves the op making it unread
TEST_F(RegionBodyEffectsTest, foldConstantMapKeysKeepsABodyThatMayRaise) {
    const std::string_view product = R"mlir(
  %pairs:2 = db.cross_product factor {
    %y = db.scan_nodes() : !nodes
    @BODY@
    db.yield %y : !nodes
  } factor {
    %key = db.constant("s" : !storage.string)
    db.yield %key : !strings
  }
  %s = db.dynamic_map_key %map, %pairs#1 : (!maps, !strings) -> !cells
  db.output(%s) : !cells
)mlir";

    const auto createFold = [] {
        return mlir::db::createFoldConstantMapKeys();
    };

    EXPECT_EQ(countAfterPass(product, "db.cross_product", RAISING_DIVISION, createFold), 1u);
    EXPECT_EQ(countAfterPass(product, "db.cross_product", SAFE_DIVISION, createFold), 0u);
    EXPECT_EQ(countOps("db.dynamic_map_key"), 0u);

    const std::string_view walk = R"mlir(
  %key = db.constant("s" : !storage.string)
  %laid = db.broadcast_constant(%key, %n) : (!strings, !nodes) -> !strings
  %paths:4 = db.explore_paths(%n, {%laid}) forward hops 1 to 2 {
  ^bb0(%source: !nodes, %edge: !edges, %end: !nodes):
    @BODY@
    %held = db.is_not_null %end : (!nodes) -> !bools
    db.yield %held : !bools
  } : (!nodes, !strings) -> (!nodes, !nodes, !db.column<!storage.path_ref>, !strings)
  %s = db.dynamic_map_key %map, %paths#3 : (!maps, !strings) -> !cells
  db.output(%s) : !cells
)mlir";

    EXPECT_EQ(countAfterPass(walk, "db.explore_paths", RAISING_DIVISION, createFold), 1u);
    EXPECT_EQ(countAfterPass(walk, "db.explore_paths", SAFE_DIVISION, createFold), 0u);
    EXPECT_EQ(countOps("db.dynamic_map_key"), 0u);
}

// The canonicalizer's driver erases a coalesce nothing reads, and drops an argument yielding
// the null literal
TEST_F(RegionBodyEffectsTest, canonicalizeCoalesceKeepsABodyThatMayRaise) {
    const std::string_view unread = R"mlir(
  %c = db.coalesce({%rows}) {
  ^bb0(%x: !cells):
    db.case_yield %x : !cells
  }, {
  ^bb0(%x: !cells):
    %xs = db.constant([1, 2])
    %l = db.list_comprehension(%xs, {%x}) {
    ^bb0(%element: !any, %tag: !tags, %row: !cells):
      @BODY@
      db.comprehension_yield %tag, %q : !any
    } : (!lists, !cells) -> !any
    db.case_yield %l : !any
  } : (!cells) -> !any
)mlir";

    EXPECT_EQ(countAfterPass(unread, "db.coalesce", RAISING_DIVISION, mlir::db::createCanonicalizeCoalesce), 1u);
    EXPECT_EQ(countAfterPass(unread, "db.coalesce", SAFE_DIVISION, mlir::db::createCanonicalizeCoalesce), 0u);

    const std::string_view nullArgument = R"mlir(
  %c = db.coalesce({%rows}) {
  ^bb0(%x: !cells):
    %xs = db.constant([1, 2])
    %l = db.list_comprehension(%xs, {%x}) {
    ^bb0(%element: !any, %tag: !tags, %row: !cells):
      @BODY@
      db.comprehension_yield %tag, %q : !any
    } : (!lists, !cells) -> !any
    %null = db.constant("" : !storage.nullable<none>)
    db.case_yield %null : !db.column<!storage.nullable<none>>
  }, {
  ^bb0(%x: !cells):
    db.case_yield %x : !cells
  } : (!cells) -> !cells
  db.output(%c) : !cells
)mlir";

    EXPECT_EQ(countAfterPass(nullArgument, "db.list_comprehension", RAISING_DIVISION, mlir::db::createCanonicalizeCoalesce), 1u);
    EXPECT_EQ(countAfterPass(nullArgument, "db.list_comprehension", SAFE_DIVISION, mlir::db::createCanonicalizeCoalesce), 0u);
}

// The count is the same for every element, so it is computed once per row, unless its body
// may raise: a row whose list is empty would then raise where it did not
TEST_F(RegionBodyEffectsTest, placeRowInvariantOpsKeepsABodyThatMayRaise) {
    const std::string_view counted = R"mlir(
  %xs = db.constant([1, 2])
  %l = db.list_comprehension(%xs, {%n}) {
  ^bb0(%x: !any, %tag: !tags, %nIn: !nodes):
    %c = db.count_subquery(%nIn) {
    ^bb0(%m: !nodes):
      @BODY@
      %row = db.row_constant %m : !nodes
      db.count_subquery_yield {%row} : {!nodes}
    } : (!nodes) -> !tags
    db.comprehension_yield %tag, %c : !tags
  } : (!lists, !nodes) -> !any
  db.output(%l) : !any
)mlir";

    const auto countHoisted = [this] {
        size_t hoisted = 0;
        _module->walk([&hoisted](mlir::db::CountSubquery count) {
            if (mlir::isa<mlir::func::FuncOp>(count->getParentOp())) {
                hoisted++;
            }
        });

        return hoisted;
    };

    ASSERT_TRUE(parse(counted, RAISING_DIVISION));
    ASSERT_TRUE(runPass(mlir::db::createPlaceRowInvariantOps));
    EXPECT_EQ(countHoisted(), 0u);

    ASSERT_TRUE(parse(counted, SAFE_DIVISION));
    ASSERT_TRUE(runPass(mlir::db::createPlaceRowInvariantOps));
    EXPECT_EQ(countHoisted(), 1u);
}

// A value nothing reads still raises on a row it cannot be computed for, though a pass folds
// what reads it
class UnreadRegionBodyRaisesTest : public WriteQueryTest {
};

TEST_F(UnreadRegionBodyRaisesTest, raisesInAListComprehensionOnlyItsSizeReads) {
    expectError("RETURN size([x IN [0] | 1 / x]) >= 0", "Attempted to divide by zero");
}

TEST_F(UnreadRegionBodyRaisesTest, raisesInAListComprehensionNothingReads) {
    expectError("WITH [x IN [0] | 1 / x] AS l RETURN 1", "Attempted to divide by zero");
    expectError("UNWIND [0] AS z WITH z, [x IN [z] | 1 / x] AS l RETURN z", "Attempted to divide by zero");
}

TEST_F(UnreadRegionBodyRaisesTest, raisesInABodyNothingReads) {
    expectError("UNWIND [0] AS z WITH z, reduce(s = 0, x IN [z] | s + 1 / x) AS l RETURN z",
                "Attempted to divide by zero");
    expectError("UNWIND [0] AS z WITH z, any(x IN [z] WHERE 1 / x = 1) AS l RETURN z", "Attempted to divide by zero");
    expectError("UNWIND [0] AS z WITH z, CASE WHEN z = 0 THEN 1 / z END AS l RETURN z", "Attempted to divide by zero");
    expectError("UNWIND [0] AS z WITH z, coalesce(null, 1 / z) AS l RETURN z", "Attempted to divide by zero");
    expectError("MATCH (n:Person) WITH n, COLLECT { MATCH (n)-->(m) RETURN 1 / 0 } AS l RETURN n.name",
                "Attempted to divide by zero");
}
