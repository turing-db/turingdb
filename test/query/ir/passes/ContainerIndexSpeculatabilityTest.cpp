#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Parser/Parser.h"

#include "DBOps.h"
#include "HashJoinQueryTest.h"
#include "StringRowSink.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

// c[i] with a type-erased index raises on a row holding neither a list read by an integer nor
// a map, a node or an edge read by a string. Where the base and the index hold one of those two
// pairs, nulls aside, it raises on no row, so it is computed once over constants and moved as a
// typed read is.
class ContainerIndexSpeculatabilityTest : public HashJoinQueryTest {
protected:
    bool isSpeculatable(std::string_view ops) {
        std::string program = R"mlir(
!integers = !db.column<i64>
!strings = !db.column<!storage.string>
!lists = !db.column<!storage.list<i64>>
!mixed = !db.column<!storage.list<!storage.any>>
!maps = !db.column<!storage.map>
!nodes = !db.column<!storage.node_id>
!edges = !db.column<!storage.edge_id>
!cells = !db.column<!storage.any>
!unresolved = !db.column<none>

func.func @main(%integer: !integers, %string: !strings, %list: !lists, %map: !maps,
                %node: !nodes, %edge: !edges, %cell: !cells) {
  )mlir";

        program += ops;
        program += "\n  return\n}\n";

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        EXPECT_TRUE(module) << ops;
        if (!module) {
            return false;
        }

        mlir::Operation* index = nullptr;
        module->walk([&index](mlir::db::ContainerIndex containerIndex) {
            index = containerIndex.getOperation();
        });

        EXPECT_TRUE(index) << ops;

        return index && mlir::isSpeculatable(index);
    }

    bool isSpeculatableOverTheElementAt(std::string_view list, std::string_view position) {
        std::string ops = "%xs = db.constant(";
        ops += list;
        ops += ")\n  %at = db.constant(";
        ops += position;
        ops += R"mlir( : i64)
  %i = db.list_index %xs, %at : (!mixed, !integers) -> !unresolved
  %r = db.container_index %list, %i : (!lists, !unresolved) -> !unresolved
)mlir";

        return isSpeculatable(ops);
    }

    void expectComputedOnce(std::string_view query) {
        std::string program;
        nlProgram(query, program);

        const size_t index = program.find("nl.container_index");
        const size_t loop = program.find("nl.for");
        ASSERT_NE(index, std::string::npos) << program;
        ASSERT_NE(loop, std::string::npos) << program;

        EXPECT_LT(index, loop) << program;
    }

    void expectComputedPerRow(std::string_view query) {
        std::string program;
        nlProgram(query, program);

        const size_t index = program.find("nl.container_index");
        const size_t loop = program.find("nl.for");
        ASSERT_NE(index, std::string::npos) << program;
        ASSERT_NE(loop, std::string::npos) << program;

        EXPECT_GT(index, loop) << program;
    }
};

TEST_F(ContainerIndexSpeculatabilityTest, readsAListByAnInteger) {
    EXPECT_TRUE(isSpeculatable("%r = db.container_index %list, %integer : (!lists, !integers) -> !unresolved"));

    EXPECT_TRUE(isSpeculatable(R"mlir(
  %rows = db.unwind_const([[1 : i64], unit, ["a" : !storage.string]]) : !cells
  %r = db.container_index %rows, %integer : (!cells, !integers) -> !unresolved
)mlir"));
}

TEST_F(ContainerIndexSpeculatabilityTest, readsAMapANodeOrAnEdgeByAString) {
    EXPECT_TRUE(isSpeculatable("%r = db.container_index %map, %string : (!maps, !strings) -> !unresolved"));
    EXPECT_TRUE(isSpeculatable("%r = db.container_index %node, %string : (!nodes, !strings) -> !unresolved"));
    EXPECT_TRUE(isSpeculatable("%r = db.container_index %edge, %string : (!edges, !strings) -> !unresolved"));

    EXPECT_TRUE(isSpeculatable(R"mlir(
  %rows = db.unwind_const([{a = 1 : i64}, unit]) : !cells
  %keys = db.unwind_const(["a" : !storage.string, unit]) : !cells
  %r = db.container_index %rows, %keys : (!cells, !cells) -> !unresolved
)mlir"));
}

TEST_F(ContainerIndexSpeculatabilityTest, raisesOnAListReadByAKeyOrAMapReadByAPosition) {
    EXPECT_FALSE(isSpeculatable("%r = db.container_index %list, %string : (!lists, !strings) -> !unresolved"));
    EXPECT_FALSE(isSpeculatable("%r = db.container_index %map, %integer : (!maps, !integers) -> !unresolved"));
    EXPECT_FALSE(isSpeculatable("%r = db.container_index %node, %integer : (!nodes, !integers) -> !unresolved"));

    EXPECT_FALSE(isSpeculatable(R"mlir(
  %rows = db.unwind_const([[1 : i64], {a = 1 : i64}]) : !cells
  %r = db.container_index %rows, %integer : (!cells, !integers) -> !unresolved
)mlir"));

    EXPECT_FALSE(isSpeculatable(R"mlir(
  %rows = db.unwind_const([[1 : i64], {a = 1 : i64}]) : !cells
  %r = db.container_index %rows, %string : (!cells, !strings) -> !unresolved
)mlir"));
}

TEST_F(ContainerIndexSpeculatabilityTest, raisesOnACell) {
    EXPECT_FALSE(isSpeculatable("%r = db.container_index %list, %cell : (!lists, !cells) -> !unresolved"));
    EXPECT_FALSE(isSpeculatable("%r = db.container_index %map, %cell : (!maps, !cells) -> !unresolved"));
    EXPECT_FALSE(isSpeculatable("%r = db.container_index %cell, %integer : (!cells, !integers) -> !unresolved"));
    EXPECT_FALSE(isSpeculatable("%r = db.container_index %cell, %string : (!cells, !strings) -> !unresolved"));
}

// [1, 'a'][0] holds the integer 1 alone, [1, 'a'][-1] the string 'a', and a position past
// either end null
TEST_F(ContainerIndexSpeculatabilityTest, readsAnIndexHeldAtALiteralPositionOfAList) {
    EXPECT_TRUE(isSpeculatableOverTheElementAt("[1, \"a\" : !storage.string]", "0"));
    EXPECT_FALSE(isSpeculatableOverTheElementAt("[1, \"a\" : !storage.string]", "1"));
    EXPECT_FALSE(isSpeculatableOverTheElementAt("[1, \"a\" : !storage.string]", "-1"));
    EXPECT_TRUE(isSpeculatableOverTheElementAt("[1, \"a\" : !storage.string]", "-2"));
    EXPECT_TRUE(isSpeculatableOverTheElementAt("[1, \"a\" : !storage.string]", "2"));
    EXPECT_TRUE(isSpeculatableOverTheElementAt("[1, \"a\" : !storage.string]", "-3"));
    EXPECT_FALSE(isSpeculatableOverTheElementAt("[\"a\" : !storage.string, 1]", "0"));

    EXPECT_FALSE(isSpeculatable(R"mlir(
  %xs = db.constant([1, "a" : !storage.string])
  %i = db.list_index %xs, %integer : (!mixed, !integers) -> !unresolved
  %r = db.container_index %list, %i : (!lists, !unresolved) -> !unresolved
)mlir"));
}

TEST_F(ContainerIndexSpeculatabilityTest, readsAnIndexHeldAtALiteralPositionOfAMadeList) {
    EXPECT_TRUE(isSpeculatable(R"mlir(
  %xs = db.make_list(%integer, %string) : (!integers, !strings) -> !mixed
  %at = db.constant(0 : i64)
  %i = db.list_index %xs, %at : (!mixed, !integers) -> !unresolved
  %r = db.container_index %list, %i : (!lists, !unresolved) -> !unresolved
)mlir"));

    EXPECT_FALSE(isSpeculatable(R"mlir(
  %xs = db.make_list(%integer, %string) : (!integers, !strings) -> !mixed
  %at = db.constant(1 : i64)
  %i = db.list_index %xs, %at : (!mixed, !integers) -> !unresolved
  %r = db.container_index %list, %i : (!lists, !unresolved) -> !unresolved
)mlir"));
}

TEST_F(ContainerIndexSpeculatabilityTest, readsAnIndexHeldAtTheHeadOrTheLastOfAList) {
    EXPECT_TRUE(isSpeculatable(R"mlir(
  %xs = db.constant([1, "a" : !storage.string])
  %i = db.head(%xs) : (!mixed) -> !unresolved
  %r = db.container_index %list, %i : (!lists, !unresolved) -> !unresolved
)mlir"));

    EXPECT_FALSE(isSpeculatable(R"mlir(
  %xs = db.constant([1, "a" : !storage.string])
  %i = db.last(%xs) : (!mixed) -> !unresolved
  %r = db.container_index %list, %i : (!lists, !unresolved) -> !unresolved
)mlir"));

    EXPECT_TRUE(isSpeculatable(R"mlir(
  %xs = db.constant(["a" : !storage.string, 1])
  %i = db.last(%xs) : (!mixed) -> !unresolved
  %r = db.container_index %list, %i : (!lists, !unresolved) -> !unresolved
)mlir"));
}

TEST_F(ContainerIndexSpeculatabilityTest, computesAPositionHeldByAListOnce) {
    const std::string_view query = "UNWIND [1, 2, 3] AS r RETURN r, [10, 20, 30][[toInteger('1'), 'a'][0]]";

    expectComputedOnce(query);
    expectRows(query, {{"1", "20"}, {"2", "20"}, {"3", "20"}});
}

TEST_F(ContainerIndexSpeculatabilityTest, computesAKeyHeldByAListOnce) {
    const std::string_view query = "UNWIND [1, 2] AS r RETURN r, {a: 5}[[toString('a'), 0][0]]";

    expectComputedOnce(query);
    expectRows(query, {{"1", "5"}, {"2", "5"}});
}

TEST_F(ContainerIndexSpeculatabilityTest, computesTheLastOfAListOnce) {
    const std::string_view query = "UNWIND [1, 2] AS r RETURN r, [10, 20, 30][last(['a', toInteger('2')])]";

    expectComputedOnce(query);
    expectRows(query, {{"1", "30"}, {"2", "30"}});
}

TEST_F(ContainerIndexSpeculatabilityTest, joinsOnAKeyHeldByAMap) {
    const std::string_view query = "UNWIND [{a: 'Remy'}, {a: 'Luc'}] AS m WITH m, {k: 'a'} AS c "
                                   "MATCH (p:Person {name: m[c.k]}) RETURN p.name";

    std::string program;
    dbProgram(query, program);
    EXPECT_TRUE(contains(program, "db.hash_join")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;

    StringRowSink sink;
    runQuery(query, sink);

    Rows rows = sink.getRows();
    std::sort(rows.begin(), rows.end());

    const Rows expected {{"Luc"}, {"Remy"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(ContainerIndexSpeculatabilityTest, keepsAPositionReadByAKeyPerRow) {
    const std::string_view query = "UNWIND [] AS r RETURN r, [10, 20, 30][['a', 1][0]]";

    expectComputedPerRow(query);
    expectRows(query, {});

    StringRowSink sink;
    EXPECT_THROW(runQuery("UNWIND [1] AS r RETURN r, [10, 20, 30][['a', 1][0]]", sink), TuringException);
}

TEST_F(ContainerIndexSpeculatabilityTest, keepsAKeyReadByAPositionPerRow) {
    const std::string_view query = "UNWIND [] AS r RETURN r, {a: 5}[[0, 'a'][0]]";

    expectComputedPerRow(query);
    expectRows(query, {});

    StringRowSink sink;
    EXPECT_THROW(runQuery("UNWIND [1] AS r RETURN r, {a: 5}[[0, 'a'][0]]", sink), TuringException);
}
