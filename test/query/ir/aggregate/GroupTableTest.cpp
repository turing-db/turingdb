#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLExecutor.h"
#include "NLProgram.h"
#include "NLTranslator.h"
#include "QueryInterpreterV3.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "Graph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "iterators/ChunkConfig.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class GroupTableTest : public WriteQueryTest {
protected:
    void expectRowsInEveryChunking(std::string_view query, const Rows& expected) {
        for (const size_t chunkSize : {size_t {2}, size_t {7}, ChunkConfig::CHUNK_SIZE}) {
            _interpreter->setChunkSize(chunkSize);
            expectRows(query, expected);
        }

        _interpreter->setChunkSize(ChunkConfig::CHUNK_SIZE);
    }

    void expectRowsInOrderInEveryChunking(std::string_view query, const Rows& expected) {
        for (const size_t chunkSize : {size_t {2}, size_t {7}, ChunkConfig::CHUNK_SIZE}) {
            _interpreter->setChunkSize(chunkSize);
            expectRowsInOrder(query, expected);
        }

        _interpreter->setChunkSize(ChunkConfig::CHUNK_SIZE);
    }

    // The hash table the group table of the query's grouped RETURN or collect maps its keys with
    void expectHashTable(std::string_view query, NLGroupHashTableKind expected) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();
        Graph* graph = system.getGraph(_graphName);

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = dbModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        const mlir::func::FuncOp dbFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(builder.getUnknownLoc());

        DBLowering lowering(&context, &view, procedures);
        const mlir::func::FuncOp nlFunction = lowering.lower(dbFunction, *nlModule);

        NLProgram program;

        LocalMemory memory;
        NLTranslator translator(&program, &memory, &view);
        translator.translate(nlFunction);

        const NLStmtContainer* stmts = program.getStmts();
        for (const NLFunctionDescriptor& stmt : stmts->stmts()) {
            if (stmt.getFunction() == &NLExecutor::runGroupAggregateReset) {
                const NLGroupAggregateResetData* reset = static_cast<const NLGroupAggregateResetData*>(stmt.getData());
                EXPECT_EQ(reset->getState()->groupTable().hashTable().getKind(), expected) << "query: " << query;
                return;
            } else if (stmt.getFunction() == &NLExecutor::runCollectReset) {
                const NLCollectResetData* reset = static_cast<const NLCollectResetData*>(stmt.getData());
                EXPECT_EQ(reset->getState()->groupTable().hashTable().getKind(), expected) << "query: " << query;
                return;
            }
        }

        FAIL() << "query: " << query << "\ngroups no row at function scope";
    }
};

TEST_F(GroupTableTest, choosesAHashTableForEachKeyShape) {
    expectHashTable("UNWIND range(1, 3) AS x RETURN collect(x)", NLGroupHashTableKind::Keyless);

    expectHashTable("MATCH (n)-->(m) RETURN n, count(m)", NLGroupHashTableKind::Fixed64);
    expectHashTable("MATCH (n) WITH count(n) AS c RETURN c, count(*)", NLGroupHashTableKind::Fixed64);
    expectHashTable("MATCH (n) RETURN n.isFrench, n.hasPhD, count(*)", NLGroupHashTableKind::Fixed64);

    expectHashTable("MATCH (n) RETURN n.age, count(*)", NLGroupHashTableKind::NullableFixed64);
    expectHashTable("UNWIND range(1, 3) AS x RETURN x % 2 AS g, count(*)", NLGroupHashTableKind::NullableFixed64);
    expectHashTable("MATCH (n) RETURN n.isFrench, count(*)", NLGroupHashTableKind::NullableFixed64);
    expectHashTable("UNWIND range(1, 3) AS x RETURN duration({seconds: x}) AS d, count(*)",
                    NLGroupHashTableKind::NullableFixed64);

    expectHashTable("MATCH (n)-->(m) RETURN n, m, count(*)", NLGroupHashTableKind::Fixed128);
    expectHashTable("MATCH (n) RETURN n.age, n.isFrench, count(*)", NLGroupHashTableKind::Fixed128);

    expectHashTable("MATCH (a)-->(b)-->(c) RETURN a, b, c, count(*)", NLGroupHashTableKind::Fixed256);
    expectHashTable("UNWIND range(1, 3) AS x RETURN x % 2 AS a, x % 3 AS b, x % 5 AS c, count(*)",
                    NLGroupHashTableKind::Fixed256);

    expectHashTable("MATCH (n) RETURN n.name, count(*)", NLGroupHashTableKind::NullableString);
    expectHashTable("MATCH (n)-[e]->() RETURN type(e), count(*)", NLGroupHashTableKind::NullableString);

    expectHashTable("MATCH (n) RETURN n.name, n.age, count(*)", NLGroupHashTableKind::Serialized);
    expectHashTable("MATCH (n) RETURN labels(n), count(*)", NLGroupHashTableKind::Serialized);
    expectHashTable("UNWIND [1, 'a'] AS c RETURN c, count(*)", NLGroupHashTableKind::Serialized);
    expectHashTable("UNWIND range(1, 3) AS x RETURN {a: x} AS m, count(*)", NLGroupHashTableKind::Serialized);
    expectHashTable("UNWIND range(1, 3) AS x RETURN point({x: x, y: 0}) AS p, count(*)", NLGroupHashTableKind::Serialized);
    expectHashTable("UNWIND range(1, 3) AS x RETURN x AS a, x + 1 AS b, x + 2 AS c, x + 3 AS d, count(*)",
                    NLGroupHashTableKind::Serialized);
}

TEST_F(GroupTableTest, emitsTheGroupsInTheOrderTheirKeysFirstAppear) {
    expectRowsInOrderInEveryChunking("UNWIND range(0, 20) AS x RETURN (x * 2) % 5 AS g, count(*)",
                                     {{"0", "5"}, {"2", "4"}, {"4", "4"}, {"1", "4"}, {"3", "4"}});
    expectRowsInOrderInEveryChunking("UNWIND range(0, 20) AS x RETURN toString((x * 2) % 5) AS g, count(*)",
                                     {{"0", "5"}, {"2", "4"}, {"4", "4"}, {"1", "4"}, {"3", "4"}});
    expectRowsInOrderInEveryChunking("UNWIND range(0, 20) AS x RETURN (x * 2) % 5 AS a, x % 2 AS b, count(*)",
                                     {{"0", "0", "3"}, {"2", "1", "2"}, {"4", "0", "2"}, {"1", "1", "2"},
                                      {"3", "0", "2"}, {"0", "1", "2"}, {"2", "0", "2"}, {"4", "1", "2"},
                                      {"1", "0", "2"}, {"3", "1", "2"}});
    expectRowsInOrderInEveryChunking("UNWIND range(0, 20) AS x RETURN [(x * 2) % 5] AS g, count(*)",
                                     {{"[0]", "5"}, {"[2]", "4"}, {"[4]", "4"}, {"[1]", "4"}, {"[3]", "4"}});
}

TEST_F(GroupTableTest, groupsTheNullsOfOneKeyTogether) {
    expectRowsInOrderInEveryChunking("UNWIND [0, null, 0, null, 1, null] AS x RETURN x, count(*)",
                                     {{"0", "2"}, {"null", "3"}, {"1", "1"}});
    expectRowsInEveryChunking("MATCH (n) RETURN n.age, count(*)", {{"32", "2"}, {"null", "16"}});
}

TEST_F(GroupTableTest, tellsANullKeyFromAZeroKey) {
    expectRowsInEveryChunking("UNWIND [[null, 0], [0, null], [0, 0], [null, null], [0, null], [null, 0]] AS p "
                              "RETURN toInteger(p[0]) AS a, toInteger(p[1]) AS b, count(*)",
                              {{"null", "0", "2"}, {"0", "null", "2"}, {"0", "0", "1"}, {"null", "null", "1"}});
    expectRowsInEveryChunking("UNWIND [[null, 0], [0, null], [0, 0], [null, null], [0, null]] AS p "
                              "RETURN toInteger(p[0]) AS a, toInteger(p[1]) AS b, toInteger(p[0]) AS c, count(*)",
                              {{"null", "0", "null", "1"}, {"0", "null", "0", "2"}, {"0", "0", "0", "1"},
                               {"null", "null", "null", "1"}});
}

TEST_F(GroupTableTest, groupsFloatsByValue) {
    expectRowsInEveryChunking("UNWIND [0.0, -0.0, 1.5, -1.5, 1.5, -0.0] AS x RETURN x, count(*)",
                              {{"0.000000", "3"}, {"1.500000", "2"}, {"-1.500000", "1"}});
}

TEST_F(GroupTableTest, groupsStringsApartFromNull) {
    expectRowsInOrderInEveryChunking("UNWIND ['b', null, 'a', '', null, 'b', '', 'ab', 'a'] AS s RETURN s, count(*)",
                                     {{"b", "2"}, {"null", "2"}, {"a", "2"}, {"", "2"}, {"ab", "1"}});
    expectRowsInEveryChunking("MATCH ()-[e]->() RETURN e.proficiency, count(*)",
                              {{"expert", "4"}, {"moderate", "1"}, {"null", "13"}});
}

TEST_F(GroupTableTest, groupsBooleans) {
    expectRowsInEveryChunking("MATCH (n) RETURN n.isFrench, count(*)",
                              {{"true", "4"}, {"false", "4"}, {"null", "10"}});
    expectRowsInEveryChunking("MATCH (n) RETURN n.isFrench, n.hasPhD, count(*)",
                              {{"true", "true", "3"}, {"true", "false", "1"}, {"false", "true", "1"},
                               {"false", "false", "3"}, {"null", "null", "10"}});
}

TEST_F(GroupTableTest, groupsEntities) {
    expectRowsInEveryChunking("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH i, count(p) AS n RETURN i.name, n",
                              {{"Computers", "2"}, {"Eighties", "1"}, {"Bio", "2"}, {"Cooking", "2"},
                               {"Ghosts", "1"}, {"Padel", "1"}, {"Animals", "1"}, {"Gym", "3"},
                               {"Travel", "1"}, {"JiuJitsu", "1"}});
    expectRowsInEveryChunking("MATCH (a)-->(b)-->(c) WITH a, c, count(b) AS n RETURN a.name, c.name, n",
                              {{"Remy", "Remy", "2"}, {"Remy", "Bio", "1"}, {"Remy", "Cooking", "1"},
                               {"Adam", "Adam", "1"}, {"Adam", "Ghosts", "1"}, {"Adam", "Computers", "1"},
                               {"Adam", "Eighties", "1"}, {"Ghosts", "Adam", "1"}, {"Ghosts", "Ghosts", "1"},
                               {"Ghosts", "Computers", "1"}, {"Ghosts", "Eighties", "1"}});
}

TEST_F(GroupTableTest, groupsDurations) {
    expectRowsInEveryChunking("UNWIND range(1, 7) AS x RETURN duration({seconds: x % 3}) AS d, count(*)",
                              {{"PT0S", "2"}, {"PT1S", "3"}, {"PT2S", "2"}});
}

TEST_F(GroupTableTest, groupsPoints) {
    expectRowsInEveryChunking("UNWIND [1, 2, 1, 3, 1] AS x RETURN point({x: x, y: 0}) AS p, count(*)",
                              {{"point({x: 1.0, y: 0.0, crs: 'cartesian'})", "3"},
                               {"point({x: 2.0, y: 0.0, crs: 'cartesian'})", "1"},
                               {"point({x: 3.0, y: 0.0, crs: 'cartesian'})", "1"}});
    expectRowsInEveryChunking("UNWIND [1, null, 1] AS x RETURN point({x: x, y: 0}) AS p, count(*)",
                              {{"point({x: 1.0, y: 0.0, crs: 'cartesian'})", "2"}, {"null", "1"}});
}

TEST_F(GroupTableTest, groupsCellsByValue) {
    expectRowsInEveryChunking("UNWIND [1, 1.0, 'a', 2, 'a', true, null, 2.5, null] AS c RETURN c, count(*)",
                              {{"1", "2"}, {"a", "2"}, {"2", "1"}, {"true", "1"}, {"null", "2"}, {"2.500000", "1"}});
}

TEST_F(GroupTableTest, groupsListsAndMaps) {
    expectRowsInEveryChunking("UNWIND [[1], [1], [1, 2], [], [], [null], [null]] AS l RETURN l, count(*)",
                              {{"[1]", "2"}, {"[1, 2]", "1"}, {"[]", "2"}, {"[null]", "2"}});
    expectRowsInEveryChunking("UNWIND range(1, 6) AS x RETURN {a: x % 2} AS m, count(*)",
                              {{"{a: 1}", "3"}, {"{a: 0}", "3"}});
}

TEST_F(GroupTableTest, collectsPerGroupAndWithoutAKey) {
    expectRowsInEveryChunking("UNWIND range(1, 9) AS x RETURN toString(x % 3) AS g, collect(x)",
                              {{"0", "[3, 6, 9]"}, {"1", "[1, 4, 7]"}, {"2", "[2, 5, 8]"}});
    expectRowsInEveryChunking("UNWIND range(1, 9) AS x RETURN x % 3 AS g, x % 2 AS h, collect(x)",
                              {{"0", "0", "[6]"}, {"0", "1", "[3, 9]"}, {"1", "0", "[4]"}, {"1", "1", "[1, 7]"},
                               {"2", "0", "[2, 8]"}, {"2", "1", "[5]"}});
    expectRowsInEveryChunking("UNWIND range(1, 4) AS x RETURN collect(x)", {{"[1, 2, 3, 4]"}});
    expectRowsInEveryChunking("UNWIND [] AS x RETURN collect(x)", {{"[]"}});
}

// Thousands of groups grow each hash table several times, and a chunk of 2 or 7 rows
// meets most keys in a later chunk than the one that created their group
TEST_F(GroupTableTest, growsOverThousandsOfGroups) {
    expectRowsInEveryChunking("UNWIND range(1, 30000) AS x WITH x % 7919 AS g, count(*) AS n "
                              "RETURN count(g), sum(n), min(n), max(n)",
                              {{"7919", "30000", "3", "4"}});
    expectRowsInEveryChunking("UNWIND range(1, 30000) AS x WITH toString(x % 7919) AS g, count(*) AS n "
                              "RETURN count(g), sum(n), min(n), max(n)",
                              {{"7919", "30000", "3", "4"}});
    expectRowsInEveryChunking("UNWIND range(1, 30000) AS x WITH x % 89 AS a, x % 97 AS b, count(*) AS n "
                              "RETURN count(*), sum(n), min(n), max(n)",
                              {{"8633", "30000", "3", "4"}});
    expectRowsInEveryChunking("UNWIND range(1, 30000) AS x WITH x % 7 AS a, x % 11 AS b, x % 13 AS c, count(*) AS n "
                              "RETURN count(*), sum(n), min(n), max(n)",
                              {{"1001", "30000", "29", "30"}});
    expectRowsInEveryChunking("UNWIND range(1, 30000) AS x WITH [x % 7919] AS g, count(*) AS n "
                              "RETURN count(*), sum(n), min(n), max(n)",
                              {{"7919", "30000", "3", "4"}});
    expectRowsInEveryChunking("UNWIND range(1, 30000) AS x WITH [x % 7919, 'a'][0] AS g, count(*) AS n "
                              "RETURN count(*), sum(n), min(n), max(n)",
                              {{"7919", "30000", "3", "4"}});
}

// A key of hundreds of bytes outgrows the buffer a row's key serializes into several times
TEST_F(GroupTableTest, groupsByKeysOfHundredsOfBytes) {
    const std::string tail(300, 'a');
    expectRowsInEveryChunking("UNWIND range(1, 12) AS x RETURN x % 2 AS a, toString(x % 3) + '" + tail + "' AS s, count(*)",
                              {{"0", "0" + tail, "2"}, {"0", "1" + tail, "2"}, {"0", "2" + tail, "2"},
                               {"1", "0" + tail, "2"}, {"1", "1" + tail, "2"}, {"1", "2" + tail, "2"}});
    expectRowsInEveryChunking("UNWIND range(1, 12) AS x WITH range(1, 40 + x % 3) AS l, count(*) AS n RETURN size(l), n",
                              {{"40", "4"}, {"41", "4"}, {"42", "4"}});
}
