#include <gtest/gtest.h>

#include <string_view>

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
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CallV3Test.h"
#include "IRTestOps.h"
#include "IRTestRows.h"

using namespace turing::test;

namespace {

// MATCH (n)-->(m) WITH n, m WHERE n:Person AND n:Founder RETURN n, m
const char* const stackedNullableLabelFilters = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) {nullable} : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %s1, %t1 = db.filter(%p, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %ls2 = db.get_node_label_set(%s1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %f = db.check_label_constraint(%ls2, ["Founder"]) {nullable} : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %s2, %t2 = db.filter(%f, {%s1, %t1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%s2, %t2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n:Person)-->(m) WITH n, m WHERE n:Founder RETURN n, m
const char* const stackedLabelFiltersOfMixedFlags = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %s1, %t1 = db.filter(%p, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %ls2 = db.get_node_label_set(%s1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %f = db.check_label_constraint(%ls2, ["Founder"]) {nullable} : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %s2, %t2 = db.filter(%f, {%s1, %t1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%s2, %t2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)-->(m) WITH n, m WHERE n:Founder RETURN n
const char* const nullableLabelFilterAfterAHop = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %ls = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %f = db.check_label_constraint(%ls, ["Founder"]) {nullable} : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %t1, %e1, %s1, %et1 = db.filter(%f, {%t, %e, %s, %et}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>)
  db.output(%s1) : !db.column<!storage.node_id>
  return
}
)mlir";

}

class TypeTestOnANullEntityTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectWriteRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runWrite(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectError(std::string_view query, std::string_view reason) {
        runQueryExpectingError(query, reason);
    }
};

TEST_F(TypeTestOnANullEntityTest, labelTestOnANullNodeIsNull) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[:NOPE]->(n) RETURN p.name, n:Person",
               {{"Remy", "null"}});
}

TEST_F(TypeTestOnANullEntityTest, negatedLabelTestOnANullNodeIsNull) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[:NOPE]->(n) RETURN p.name, NOT n:Person",
               {{"Remy", "null"}});
}

TEST_F(TypeTestOnANullEntityTest, negatedLabelTestOnANullNodeDropsTheRow) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[:NOPE]->(n) WITH p, n WHERE NOT n:Person RETURN p.name",
               {});
}

TEST_F(TypeTestOnANullEntityTest, edgeTypeTestOnANullEdgeIsNull) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[e:NOPE]->(n) RETURN p.name, e:NOPE",
               {{"Remy", "null"}});
}

TEST_F(TypeTestOnANullEntityTest, labelTestIsNullOnlyOnTheMissedRows) {
    expectRows("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS_WELL]->(b) RETURN a.name, b:Person",
               {{"Adam", "true"},
                {"Cyrus", "null"},
                {"Doruk", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Maxime", "null"},
                {"Remy", "true"},
                {"Suhas", "null"}});
}

TEST_F(TypeTestOnANullEntityTest, edgeTypeTestIsNullOnlyOnTheMissedRows) {
    expectRows("MATCH (a:Person) OPTIONAL MATCH (a)-[e:KNOWS_WELL]->(b) RETURN a.name, e:KNOWS_WELL",
               {{"Adam", "true"},
                {"Cyrus", "null"},
                {"Doruk", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Maxime", "null"},
                {"Remy", "true"},
                {"Suhas", "null"}});
}

TEST_F(TypeTestOnANullEntityTest, negatedLabelTestKeepsOnlyTheNonMatchingRows) {
    expectRows("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS_WELL]->(b) WITH a, b WHERE NOT b:Interest RETURN a.name",
               {{"Adam"}, {"Remy"}});
}

TEST_F(TypeTestOnANullEntityTest, labelTestOnANullNodeFollowsThreeValuedLogic) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[:NOPE]->(n) "
               "RETURN n:Person OR true, n:Person AND false, n:Person OR false, n:Person IS NULL",
               {{"true", "false", "null", "true"}});
}

TEST_F(TypeTestOnANullEntityTest, combinedTypeTestsOnANullEntityAreNull) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[e:NOPE]->(n) "
               "RETURN NOT (n:Person OR n:Interest), n:Person AND n:Founder, NOT (e:NOPE OR e:KNOWS_WELL)",
               {{"null", "null", "null"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[e:NOPE]->(n) "
               "WITH p, n, e WHERE NOT (n:Person OR n:Interest) OR NOT (e:NOPE OR e:KNOWS_WELL) RETURN p.name",
               {});
}

TEST_F(TypeTestOnANullEntityTest, labelTestOnAWrittenNodeIsNullWhereTheSubqueryMissed) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "OPTIONAL CALL (name) { CREATE (n:Person {name: name}) WITH n WHERE name = 'Nia' RETURN n } "
                    "RETURN name, n:Person",
                    {{"Nia", "true"}, {"Remy", "null"}});
}

TEST_F(TypeTestOnANullEntityTest, labelTestOnANullVariableIsNull) {
    expectRows("WITH null AS n RETURN n:Person", {{"null"}});
    expectRows("WITH null AS n RETURN n:Person:Founder", {{"null"}});
    expectRows("UNWIND [null, null] AS n RETURN n:Person", {{"null"}, {"null"}});
}

TEST_F(TypeTestOnANullEntityTest, edgeTypeTestOnANullVariableIsNull) {
    expectRows("WITH null AS r RETURN r:KNOWS_WELL", {{"null"}});
}

TEST_F(TypeTestOnANullEntityTest, labelTestOnANullVariableFollowsThreeValuedLogic) {
    expectRows("WITH null AS n RETURN NOT n:Person, n:Person OR true, n:Person AND false, n:Person IS NULL",
               {{"null", "true", "false", "true"}});
}

TEST_F(TypeTestOnANullEntityTest, labelTestOnANullVariableIsNullBesideRealRows) {
    expectRows("MATCH (p:Person) WITH p, null AS n RETURN p.name, n:Person",
               {{"Adam", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Maxime", "null"},
                {"Remy", "null"},
                {"Suhas", "null"}});
}

TEST_F(TypeTestOnANullEntityTest, labelTestOnANullVariableKeepsOnlyTheTrueRows) {
    expectRows("MATCH (p:Person) WITH p, null AS n WHERE n:Person OR p.name = 'Remy' RETURN p.name",
               {{"Remy"}});
}

TEST_F(TypeTestOnANullEntityTest, nullWhereKeepsNoRow) {
    expectRows("MATCH (n) WHERE null RETURN n", {});
    expectRows("MATCH (n:Person) WHERE null RETURN n.name", {});
    expectRows("MATCH (n:Person WHERE null) RETURN n.name", {});
    expectRows("MATCH (p:Person) WITH p WHERE null RETURN p.name", {});
    expectRows("MATCH (p:Person) WITH count(p) AS c WHERE null RETURN c", {});
    expectRows("CALL db.labels() YIELD label WHERE null RETURN label", {});
}

TEST_F(TypeTestOnANullEntityTest, nullTypedWhereKeepsNoRow) {
    expectRows("WITH null AS x WITH x WHERE x.name RETURN x", {});
    expectRows("MATCH (p:Person) WITH p, null AS n WHERE n:Person RETURN p.name", {});
    expectRows("MATCH (p:Person) WHERE null:Person RETURN p.name", {});
}

TEST_F(TypeTestOnANullEntityTest, nullWhereOfAnOptionalMatchPadsTheRow) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-->(m) WHERE null RETURN p.name, m.name",
               {{"Remy", "null"}});
}

TEST_F(TypeTestOnANullEntityTest, nullWhereInASubqueryExpressionMatchesNothing) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "RETURN EXISTS { MATCH (p)-->(m) WHERE null }, COUNT { MATCH (p)-->(m) WHERE null }",
               {{"false", "0"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN [(p)-->(m) WHERE null | m.name]", {{"[]"}});
}

TEST_F(TypeTestOnANullEntityTest, nullWhereOfAQuantifiedPathMatchesNothing) {
    expectRows("MATCH (a:Person {name: 'Remy'}) ((x)-[r]->(y) WHERE null){1,2} (b) RETURN b.name", {});
}

TEST_F(TypeTestOnANullEntityTest, nonBooleanWhereIsRejected) {
    expectError("MATCH (n) WHERE 1 RETURN n", "WHERE expression must be a boolean");
    expectError("MATCH (n:Person) WITH n WHERE 'a' RETURN n.name", "WHERE expression must be a boolean");
    expectError("CALL db.labels() YIELD label WHERE label RETURN label", "WHERE expression must be a boolean");
}

TEST_F(TypeTestOnANullEntityTest, labelTestOnTheNullLiteralIsNull) {
    expectRows("RETURN null:Person", {{"null"}});
    expectRows("RETURN null:Person:Founder, NOT null:Person, (null):Person", {{"null", "null", "null"}});
    expectRows("WITH null AS x RETURN x.name:Person, (x):Person", {{"null", "null"}});
}

TEST_F(TypeTestOnANullEntityTest, labelTestOnAParenthesisedEntity) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN (n):Person, (n):Interest", {{"true", "false"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[e:NOPE]->(n) RETURN (n):Person, (e):NOPE",
               {{"null", "null"}});
}

TEST_F(TypeTestOnANullEntityTest, labelTestOnAnEntityElement) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [n][0]:Person, [n][0]:Interest, [n][1]:Person",
               {{"true", "false", "null"}});
    expectRows("MATCH p = (a:Person {name: 'Remy'})-[:KNOWS_WELL]->(b) "
               "RETURN nodes(p)[1]:Person, relationships(p)[0]:KNOWS_WELL, relationships(p)[0]:NOPE",
               {{"true", "true", "false"}});
}

TEST_F(TypeTestOnANullEntityTest, labelTestOnAnEntityExpression) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN (CASE WHEN true THEN n END):Person, coalesce(n, n):Interest",
               {{"true", "false"}});
    expectRows("MATCH (n:Person) RETURN collect(n)[0]:Person", {{"true"}});
    expectRows("MATCH (n) WHERE [n][0]:Person RETURN count(*)", {{"8"}});
    expectRows("MATCH (n:Person) WITH [n][0]:Founder AS founder RETURN founder, count(*)",
               {{"false", "6"}, {"true", "2"}});
}

TEST_F(TypeTestOnANullEntityTest, labelTestOnAPaddedEntityElementIsNull) {
    expectRows("MATCH (p:Person {name: 'Remy'}) OPTIONAL MATCH (p)-[e:NOPE]->(n) RETURN [n][0]:Person, [e][0]:NOPE",
               {{"null", "null"}});
}

TEST_F(TypeTestOnANullEntityTest, labelTestOnAValueIsRejected) {
    expectError("RETURN 1:Person", "needs a node or an edge, not 'Integer'");
    expectError("RETURN 'a':Person", "needs a node or an edge, not 'String'");
    expectError("MATCH (n:Person) RETURN n.name:Person", "needs a node or an edge, not 'String'");
}

class TypeTestFlagThroughLabelFusionTest : public TuringTest {
protected:
    void initialize() override {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    void fuse(const char* programText, mlir::OwningOpRef<mlir::ModuleOp>& module) {
        module = mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
        ASSERT_TRUE(module);

        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFuseLabelPredicates());
        ASSERT_TRUE(mlir::succeeded(passManager.run(*module)));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));
    }

    mlir::MLIRContext _context;
};

TEST_F(TypeTestFlagThroughLabelFusionTest, stackedFiltersKeepTheFlag) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    fuse(stackedNullableLabelFilters, module);

    llvm::SmallVector<mlir::db::CheckLabelConstraint> checks = collect<mlir::db::CheckLabelConstraint>(module.get());
    ASSERT_EQ(checks.size(), 1u);
    EXPECT_TRUE(checks.front().getNullable());
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(TypeTestFlagThroughLabelFusionTest, stackedFiltersOfMixedFlagsKeepTheFlag) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    fuse(stackedLabelFiltersOfMixedFlags, module);

    llvm::SmallVector<mlir::db::CheckLabelConstraint> checks = collect<mlir::db::CheckLabelConstraint>(module.get());
    ASSERT_EQ(checks.size(), 1u);
    EXPECT_TRUE(checks.front().getNullable());
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(TypeTestFlagThroughLabelFusionTest, filterHoistedAboveAHopKeepsTheFlag) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    fuse(nullableLabelFilterAfterAHop, module);

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(module.get());
    ASSERT_EQ(hops.size(), 1u);

    mlir::db::FilterOp aboveHop = hops.front().getInputNodes().getDefiningOp<mlir::db::FilterOp>();
    ASSERT_TRUE(aboveHop);
    mlir::db::CheckLabelConstraint check = aboveHop.getMask().getDefiningOp<mlir::db::CheckLabelConstraint>();
    ASSERT_TRUE(check);
    EXPECT_TRUE(check.getNullable());
}
