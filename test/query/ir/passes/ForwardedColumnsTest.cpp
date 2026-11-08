#include <gtest/gtest.h>

#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "StorageDialect.h"

#include "IRTestOps.h"

using namespace turing::test;

// A condition region of a db.lazy_case, or an argument region of a db.coalesce, forwards
// columns to every region after it, which takes them as leading arguments
class ForwardedColumnsTest : public ::testing::Test {
protected:
    ForwardedColumnsTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    void expectRejected(const char* programText, const std::string& reason) {
        std::string messages;
        mlir::ScopedDiagnosticHandler handler(&_context, [&messages](mlir::Diagnostic& diagnostic) {
            messages += diagnostic.str();
            return mlir::success();
        });

        EXPECT_FALSE(parse(programText));
        EXPECT_NE(messages.find(reason), std::string::npos) << messages;
    }

    mlir::OwningOpRef<mlir::ModuleOp> runPass(const char* programText, std::unique_ptr<mlir::Pass> pass) {
        mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        if (!module) {
            return {};
        }

        mlir::PassManager passManager(&_context);
        passManager.addPass(std::move(pass));

        if (mlir::failed(passManager.run(*module)) || mlir::failed(mlir::verify(*module))) {
            return {};
        }

        return module;
    }

    static mlir::db::CaseYield yieldOf(mlir::Region& region) {
        return mlir::cast<mlir::db::CaseYield>(region.front().getTerminator());
    }

    mlir::MLIRContext _context;
};

// MATCH (n) RETURN CASE WHEN n.isFrench THEN 1 WHEN n:Founder THEN 2 ELSE n:Exotic END
const char* const caseForwardingTheLabelSet = R"mlir(
!nodes = !db.column<!storage.node_id>
!french = !db.column<!storage.nullable<i1>>
!labels = !db.column<!storage.labelset_id>
!bools = !db.column<!storage.bool>
!ints = !db.column<i64>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %french = db.get_node_properties(%n, "isFrench") : (!nodes) -> !french
  %one = db.constant(1 : i64)
  %two = db.constant(2 : i64)
  %r = db.lazy_case({%n, %french}) {
  ^bb0(%nIn: !nodes, %frenchIn: !french):
    db.case_yield %frenchIn : !french
  }, {
  ^bb0(%nIn: !nodes, %frenchIn: !french):
    db.case_yield %one : !ints
  }, {
  ^bb0(%nIn: !nodes, %frenchIn: !french):
    %l = db.get_node_label_set(%nIn) : (!nodes) -> !labels
    %c = db.check_label_constraint(%l, ["Founder"]) : (!labels) -> !bools
    db.case_yield %c : !bools forwarding(%l : !labels)
  }, {
  ^bb0(%lIn: !labels, %nIn: !nodes, %frenchIn: !french):
    db.case_yield %two : !ints
  }, {
  ^bb0(%lIn: !labels, %nIn: !nodes, %frenchIn: !french):
    %e = db.check_label_constraint(%lIn, ["Exotic"]) : (!labels) -> !bools
    db.case_yield %e : !bools
  } : (!nodes, !french) -> !db.column<none>
  db.output(%r) : !db.column<none>
  return
}
)mlir";

TEST_F(ForwardedColumnsTest, verifiesTheRegionsTakingAForwardedColumn) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(caseForwardingTheLabelSet);
    ASSERT_TRUE(module);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module->print(stream);

    EXPECT_TRUE(parse(printed.c_str())) << printed;
}

const char* const valueRegionForwarding = R"mlir(
!nodes = !db.column<!storage.node_id>
!french = !db.column<!storage.nullable<i1>>
!labels = !db.column<!storage.labelset_id>
!bools = !db.column<!storage.bool>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %french = db.get_node_properties(%n, "isFrench") : (!nodes) -> !french
  %r = db.lazy_case({%n, %french}) {
  ^bb0(%nIn: !nodes, %frenchIn: !french):
    db.case_yield %frenchIn : !french
  }, {
  ^bb0(%nIn: !nodes, %frenchIn: !french):
    %l = db.get_node_label_set(%nIn) : (!nodes) -> !labels
    %c = db.check_label_constraint(%l, ["Founder"]) : (!labels) -> !bools
    db.case_yield %c : !bools forwarding(%l : !labels)
  } : (!nodes, !french) -> !db.column<none>
  db.output(%r) : !db.column<none>
  return
}
)mlir";

TEST_F(ForwardedColumnsTest, rejectsAValueRegionForwardingAColumn) {
    expectRejected(valueRegionForwarding, "region 1 forwards columns, but no region after it takes them");
}

const char* const elseMissingTheForwardedColumn = R"mlir(
!nodes = !db.column<!storage.node_id>
!french = !db.column<!storage.nullable<i1>>
!labels = !db.column<!storage.labelset_id>
!bools = !db.column<!storage.bool>
!ints = !db.column<i64>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %french = db.get_node_properties(%n, "isFrench") : (!nodes) -> !french
  %one = db.constant(1 : i64)
  %r = db.lazy_case({%n, %french}) {
  ^bb0(%nIn: !nodes, %frenchIn: !french):
    %l = db.get_node_label_set(%nIn) : (!nodes) -> !labels
    %c = db.check_label_constraint(%l, ["Founder"]) : (!labels) -> !bools
    db.case_yield %c : !bools forwarding(%l : !labels)
  }, {
  ^bb0(%lIn: !labels, %nIn: !nodes, %frenchIn: !french):
    db.case_yield %one : !ints
  }, {
  ^bb0(%nIn: !nodes, %frenchIn: !french):
    db.case_yield %frenchIn : !french
  } : (!nodes, !french) -> !db.column<none>
  db.output(%r) : !db.column<none>
  return
}
)mlir";

TEST_F(ForwardedColumnsTest, rejectsARegionMissingAForwardedColumn) {
    expectRejected(elseMissingTheForwardedColumn, "region 2 takes one argument per forwarded and per carried column, expected 3 but has 2");
}

// MATCH (n) RETURN CASE WHEN n.isFrench THEN 1 WHEN n:Founder THEN 2 ELSE 3 END, with the
// label set forwarded to regions that no longer read it
const char* const caseForwardingAnUnreadColumn = R"mlir(
!nodes = !db.column<!storage.node_id>
!french = !db.column<!storage.nullable<i1>>
!labels = !db.column<!storage.labelset_id>
!bools = !db.column<!storage.bool>
!ints = !db.column<i64>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %french = db.get_node_properties(%n, "isFrench") : (!nodes) -> !french
  %one = db.constant(1 : i64)
  %two = db.constant(2 : i64)
  %three = db.constant(3 : i64)
  %r = db.lazy_case({%n, %french}) {
  ^bb0(%nIn: !nodes, %frenchIn: !french):
    db.case_yield %frenchIn : !french
  }, {
  ^bb0(%nIn: !nodes, %frenchIn: !french):
    db.case_yield %one : !ints
  }, {
  ^bb0(%nIn: !nodes, %frenchIn: !french):
    %l = db.get_node_label_set(%nIn) : (!nodes) -> !labels
    %c = db.check_label_constraint(%l, ["Founder"]) : (!labels) -> !bools
    db.case_yield %c : !bools forwarding(%l : !labels)
  }, {
  ^bb0(%lIn: !labels, %nIn: !nodes, %frenchIn: !french):
    db.case_yield %two : !ints
  }, {
  ^bb0(%lIn: !labels, %nIn: !nodes, %frenchIn: !french):
    db.case_yield %three : !ints
  } : (!nodes, !french) -> !db.column<none>
  db.output(%r) : !db.column<none>
  return
}
)mlir";

TEST_F(ForwardedColumnsTest, trimsAForwardedColumnNoLaterRegionReads) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = runPass(caseForwardingAnUnreadColumn, mlir::db::createTrimUnreadColumns());
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::LazyCase> cases = collect<mlir::db::LazyCase>(*module);
    ASSERT_EQ(cases.size(), 1u);

    const mlir::MutableArrayRef<mlir::Region> branches = cases.front().getBranches();
    EXPECT_TRUE(yieldOf(branches[2]).getForwarded().empty());
    EXPECT_EQ(branches[3].getNumArguments(), 2u);
    EXPECT_EQ(branches[4].getNumArguments(), 2u);
}

// MATCH (n) RETURN CASE WHEN n:Founder THEN 1 ELSE n:Exotic END, before the first condition,
// which every row reaches, is computed in front of the op
const char* const firstConditionForwarding = R"mlir(
!nodes = !db.column<!storage.node_id>
!labels = !db.column<!storage.labelset_id>
!bools = !db.column<!storage.bool>
!ints = !db.column<i64>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %one = db.constant(1 : i64)
  %r = db.lazy_case({%n}) {
  ^bb0(%nIn: !nodes):
    %l = db.get_node_label_set(%nIn) : (!nodes) -> !labels
    %c = db.check_label_constraint(%l, ["Founder"]) : (!labels) -> !bools
    db.case_yield %c : !bools forwarding(%l : !labels)
  }, {
  ^bb0(%lIn: !labels, %nIn: !nodes):
    db.case_yield %one : !ints
  }, {
  ^bb0(%lIn: !labels, %nIn: !nodes):
    %e = db.check_label_constraint(%lIn, ["Exotic"]) : (!labels) -> !bools
    db.case_yield %e : !bools
  } : (!nodes) -> !db.column<none>
  db.output(%r) : !db.column<none>
  return
}
)mlir";

TEST_F(ForwardedColumnsTest, carriesTheColumnsAHoistedFirstRegionForwards) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = runPass(firstConditionForwarding, mlir::db::createHoistUnconditionalRegions());
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::LazyCase> cases = collect<mlir::db::LazyCase>(*module);
    ASSERT_EQ(cases.size(), 1u);
    mlir::db::LazyCase caseOp = cases.front();

    llvm::SmallVector<mlir::db::GetNodeLabelSet> reads = collect<mlir::db::GetNodeLabelSet>(*module);
    ASSERT_EQ(reads.size(), 1u);
    EXPECT_EQ(reads.front()->getBlock(), caseOp->getBlock());

    const mlir::MutableArrayRef<mlir::Region> branches = caseOp.getBranches();
    EXPECT_TRUE(yieldOf(branches[0]).getForwarded().empty());

    mlir::Block& elseBody = branches[2].front();
    mlir::db::CheckLabelConstraint exotic = mlir::cast<mlir::db::CheckLabelConstraint>(elseBody.front());
    const mlir::BlockArgument labels = mlir::cast<mlir::BlockArgument>(exotic->getOperand(0));

    const mlir::OperandRange carried = caseOp.getColumnsToFilter();
    ASSERT_EQ(elseBody.getNumArguments(), carried.size());
    EXPECT_EQ(carried[labels.getArgNumber()], reads.front().getResult());
}

// MATCH (n) RETURN coalesce(n.isReal, <null, forwarding the label set>, n:Interest)
const char* const nullArgumentForwarding = R"mlir(
!nodes = !db.column<!storage.node_id>
!real = !db.column<!storage.nullable<i1>>
!null = !db.column<!storage.nullable<none>>
!labels = !db.column<!storage.labelset_id>
!bools = !db.column<!storage.bool>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %r = db.coalesce({%n}) {
  ^bb0(%nIn: !nodes):
    %real = db.get_node_properties(%nIn, "isReal") : (!nodes) -> !real
    db.case_yield %real : !real
  }, {
  ^bb0(%nIn: !nodes):
    %l = db.get_node_label_set(%nIn) : (!nodes) -> !labels
    %null = db.constant("" : !storage.nullable<none>)
    db.case_yield %null : !null forwarding(%l : !labels)
  }, {
  ^bb0(%lIn: !labels, %nIn: !nodes):
    %i = db.check_label_constraint(%lIn, ["Interest"]) : (!labels) -> !bools
    db.case_yield %i : !bools
  } : (!nodes) -> !db.column<none>
  db.output(%r) : !db.column<none>
  return
}
)mlir";

TEST_F(ForwardedColumnsTest, keepsANullArgumentThatForwardsAColumn) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = runPass(nullArgumentForwarding, mlir::db::createCanonicalizeCoalesce());
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::Coalesce> coalesces = collect<mlir::db::Coalesce>(*module);
    ASSERT_EQ(coalesces.size(), 1u);
    EXPECT_EQ(coalesces.front().getArguments().size(), 3u);
}

// MATCH (n) RETURN coalesce(n.dob, <'x', forwarding the label set>, n.name)
const char* const literalArgumentForwarding = R"mlir(
!nodes = !db.column<!storage.node_id>
!strings = !db.column<!storage.nullable<!storage.string>>
!labels = !db.column<!storage.labelset_id>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %r = db.coalesce({%n}) {
  ^bb0(%nIn: !nodes):
    %dob = db.get_node_properties(%nIn, "dob") : (!nodes) -> !strings
    db.case_yield %dob : !strings
  }, {
  ^bb0(%nIn: !nodes):
    %l = db.get_node_label_set(%nIn) : (!nodes) -> !labels
    %x = db.constant("x" : !storage.string)
    db.case_yield %x : !db.column<!storage.string> forwarding(%l : !labels)
  }, {
  ^bb0(%lIn: !labels, %nIn: !nodes):
    %name = db.get_node_properties(%nIn, "name") : (!nodes) -> !strings
    db.case_yield %name : !strings
  } : (!nodes) -> !strings
  db.output(%r) : !strings
  return
}
)mlir";

TEST_F(ForwardedColumnsTest, dropsTheForwardsOfTheLastArgumentLeft) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = runPass(literalArgumentForwarding, mlir::db::createCanonicalizeCoalesce());
    ASSERT_TRUE(module);

    llvm::SmallVector<mlir::db::Coalesce> coalesces = collect<mlir::db::Coalesce>(*module);
    ASSERT_EQ(coalesces.size(), 1u);

    const mlir::MutableArrayRef<mlir::Region> arguments = coalesces.front().getArguments();
    ASSERT_EQ(arguments.size(), 2u);
    EXPECT_TRUE(yieldOf(arguments[1]).getForwarded().empty());
}
