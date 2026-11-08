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

class GroupAggregateStateTest : public WriteQueryTest {
protected:
    void expectRowsInEveryChunking(std::string_view query, const Rows& expected) {
        for (const size_t chunkSize : {size_t {2}, size_t {7}, ChunkConfig::CHUNK_SIZE}) {
            _interpreter->setChunkSize(chunkSize);
            expectRows(query, expected);
        }

        _interpreter->setChunkSize(ChunkConfig::CHUNK_SIZE);
    }

    // The values the aggregates of the query's grouped RETURN copied into their store and
    // still hold there once it has run
    void foldedValueCount(std::string_view query, size_t chunkSize, size_t& count) {
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
        program.setChunkSize(chunkSize);

        LocalMemory memory;
        NLTranslator translator(&program, &memory, &view);
        translator.translate(nlFunction);

        NullSink sink;
        NLExecutor executor(&view, &program, &sink);
        executor.run();

        const NLStmtContainer* stmts = program.getStmts();
        for (const NLFunctionDescriptor& stmt : stmts->stmts()) {
            if (stmt.getFunction() == &NLExecutor::runGroupAggregateReset) {
                const NLGroupAggregateResetData* reset = static_cast<const NLGroupAggregateResetData*>(stmt.getData());
                count = reset->getState()->aggregates().getValues().getValueCount();
                return;
            }
        }

        FAIL() << "query: " << query << "\nruns no grouped aggregation at function scope";
    }
};

TEST_F(GroupAggregateStateTest, sumAndAvgOfIntegerCellsCopyNothing) {
    size_t count = 0;
    foldedValueCount("UNWIND range(1, 40) AS x WITH x % 4 AS g, [x, 'a'][0] AS c RETURN g, sum(c), avg(c)", 8, count);

    EXPECT_EQ(count, 0);
}

TEST_F(GroupAggregateStateTest, sumAndAvgOfFloatCellsCopyNothing) {
    size_t count = 0;
    foldedValueCount("UNWIND range(1, 40) AS x WITH x % 4 AS g, [toFloat(x), 'a'][0] AS c RETURN g, sum(c), avg(c)", 8, count);

    EXPECT_EQ(count, 0);
}

TEST_F(GroupAggregateStateTest, minAndMaxOfNumberCellsCopyNothing) {
    size_t count = 0;
    foldedValueCount("UNWIND range(1, 40) AS x WITH x % 4 AS g, [x, 'a'][0] AS c, [toFloat(x), 'a'][0] AS f "
                     "RETURN g, min(c), max(c), min(f), max(f)",
                     8,
                     count);

    EXPECT_EQ(count, 0);
}

TEST_F(GroupAggregateStateTest, maxOfStringCellsKeepsItsExtremesInTheStore) {
    size_t count = 0;
    foldedValueCount("UNWIND range(1, 40) AS x WITH x % 4 AS g, [toString(x), 1][0] AS c RETURN g, max(c)", 8, count);

    EXPECT_GT(count, 0);
}

TEST_F(GroupAggregateStateTest, countsEveryKindOfRow) {
    expectRowsInEveryChunking("MATCH path = (p:Person)-[e:INTERESTED_IN]->(i) "
                              "RETURN p.name, count(*), count(e), count(path), count(e.duration), "
                              "count(DISTINCT e.duration), count(DISTINCT i)",
                              {{"Remy", "3", "3", "3", "2", "1", "3"},
                               {"Adam", "2", "2", "2", "0", "0", "2"},
                               {"Maxime", "2", "2", "2", "0", "0", "2"},
                               {"Luc", "2", "2", "2", "2", "2", "2"},
                               {"Martina", "1", "1", "1", "1", "1", "1"},
                               {"Cyrus", "2", "2", "2", "0", "0", "2"},
                               {"Doruk", "1", "1", "1", "0", "0", "1"},
                               {"Suhas", "2", "2", "2", "0", "0", "2"}});
}

TEST_F(GroupAggregateStateTest, countsTheMatchesAnOptionalMatchMissed) {
    expectRowsInEveryChunking("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name, count(f), count(*)",
                              {{"Remy", "1", "1"},
                               {"Adam", "1", "1"},
                               {"Maxime", "0", "1"},
                               {"Luc", "0", "1"},
                               {"Martina", "0", "1"},
                               {"Cyrus", "0", "1"},
                               {"Doruk", "0", "1"},
                               {"Suhas", "0", "1"}});
}

TEST_F(GroupAggregateStateTest, reducesIntegerPropertiesSkippingNulls) {
    expectRowsInEveryChunking("MATCH (p:Person)-[e:INTERESTED_IN]->() "
                              "RETURN p.name, sum(e.duration), avg(e.duration), min(e.duration), max(e.duration), "
                              "stDev(e.duration), stDevP(e.duration)",
                              {{"Remy", "40", "20.000000", "20", "20", "0.000000", "0.000000"},
                               {"Adam", "0", "null", "null", "null", "0.000000", "0.000000"},
                               {"Maxime", "0", "null", "null", "null", "0.000000", "0.000000"},
                               {"Luc", "35", "17.500000", "15", "20", "3.535534", "2.500000"},
                               {"Martina", "10", "10.000000", "10", "10", "0.000000", "0.000000"},
                               {"Cyrus", "0", "null", "null", "null", "0.000000", "0.000000"},
                               {"Doruk", "0", "null", "null", "null", "0.000000", "0.000000"},
                               {"Suhas", "0", "null", "null", "null", "0.000000", "0.000000"}});
}

TEST_F(GroupAggregateStateTest, keepsTheExtremeStrings) {
    expectRowsInEveryChunking("MATCH (p:Person)-[:INTERESTED_IN]->(i) RETURN p.name, min(i.name), max(i.name)",
                              {{"Remy", "Computers", "Ghosts"},
                               {"Adam", "Bio", "Cooking"},
                               {"Maxime", "Bio", "Padel"},
                               {"Luc", "Animals", "Computers"},
                               {"Martina", "Cooking", "Cooking"},
                               {"Cyrus", "Gym", "Travel"},
                               {"Doruk", "Gym", "Gym"},
                               {"Suhas", "Gym", "JiuJitsu"}});
}

TEST_F(GroupAggregateStateTest, reducesFloatsBesideTheirDistinctValues) {
    expectRowsInEveryChunking("UNWIND [[1, 2.0], [1, 2.0], [1, null], [1, 4.0], [2, null]] AS p "
                              "WITH toInteger(p[0]) AS g, toFloat(p[1]) AS v "
                              "RETURN g, count(v), count(DISTINCT v), sum(v), sum(DISTINCT v), avg(v), avg(DISTINCT v), "
                              "min(v), max(v), stDev(v), stDevP(DISTINCT v)",
                              {{"1", "3", "2", "8.000000", "6.000000", "2.666667", "3.000000",
                                "2.000000", "4.000000", "1.154701", "1.000000"},
                               {"2", "0", "0", "0.000000", "0.000000", "null", "null",
                                "null", "null", "0.000000", "0.000000"}});
}

TEST_F(GroupAggregateStateTest, reducesDurations) {
    expectRowsInEveryChunking("UNWIND range(1, 9) AS x WITH x % 3 AS g, duration({seconds: x}) AS d "
                              "RETURN g, sum(d), avg(d), min(d), max(d), count(DISTINCT d)",
                              {{"0", "PT18S", "PT6S", "PT3S", "PT9S", "3"},
                               {"1", "PT12S", "PT4S", "PT1S", "PT7S", "3"},
                               {"2", "PT15S", "PT5S", "PT2S", "PT8S", "3"}});
}

TEST_F(GroupAggregateStateTest, reducesDistinctDurations) {
    expectRowsInEveryChunking("UNWIND [[1, 1], [1, 1], [1, 3], [2, 4], [2, 4]] AS p "
                              "WITH toInteger(p[0]) AS g, duration({seconds: toInteger(p[1])}) AS d "
                              "RETURN g, sum(DISTINCT d), avg(DISTINCT d), sum(d), avg(d)",
                              {{"1", "PT4S", "PT2S", "PT5S", "PT1.666666S"},
                               {"2", "PT4S", "PT4S", "PT8S", "PT4S"}});
}

TEST_F(GroupAggregateStateTest, countsListsAndMaps) {
    expectRowsInEveryChunking("UNWIND range(1, 8) AS x WITH x % 2 AS g, [x % 4 < 2] AS l, {a: x % 4 < 2} AS m "
                              "RETURN g, count(l), count(DISTINCT l), min(l), max(l), count(m), count(DISTINCT m)",
                              {{"0", "4", "2", "[false]", "[true]", "4", "2"},
                               {"1", "4", "2", "[false]", "[true]", "4", "2"}});
}

TEST_F(GroupAggregateStateTest, reducesIntegerCells) {
    expectRowsInEveryChunking("UNWIND range(1, 12) AS x WITH x % 3 AS g, [x, 'a'][0] AS c "
                              "RETURN g, count(c), sum(c), avg(c), min(c), max(c), stDev(c), stDevP(c)",
                              {{"0", "4", "30", "7.500000", "3", "12", "3.872983", "3.354102"},
                               {"1", "4", "22", "5.500000", "1", "10", "3.872983", "3.354102"},
                               {"2", "4", "26", "6.500000", "2", "11", "3.872983", "3.354102"}});
}

TEST_F(GroupAggregateStateTest, reducesFloatCells) {
    expectRowsInEveryChunking("UNWIND range(1, 12) AS x WITH x % 3 AS g, [toFloat(x), 'a'][0] AS c "
                              "RETURN g, count(c), sum(c), avg(c), min(c), max(c)",
                              {{"0", "4", "30.000000", "7.500000", "3.000000", "12.000000"},
                               {"1", "4", "22.000000", "5.500000", "1.000000", "10.000000"},
                               {"2", "4", "26.000000", "6.500000", "2.000000", "11.000000"}});
}

// A group's sum of cells is an integer until a float joins it, in whichever chunk that is
TEST_F(GroupAggregateStateTest, sumsIntegerCellsThenFloatCells) {
    expectRowsInEveryChunking("UNWIND [[1, 1], [1, 2], [2, 0.5], [1, 3.5], [2, 1], [3, 4], [3, 5]] AS p "
                              "WITH toInteger(p[0]) AS g, p[1] AS c RETURN g, sum(c), avg(c)",
                              {{"1", "6.500000", "2.166667"},
                               {"2", "1.500000", "0.750000"},
                               {"3", "9", "4.500000"}});
}

TEST_F(GroupAggregateStateTest, reducesDurationCellsSkippingNulls) {
    expectRowsInEveryChunking("UNWIND [[1, duration({seconds: 1})], [1, null], [2, duration({minutes: 1})], "
                              "[1, duration({seconds: 2})], [3, null]] AS p "
                              "WITH toInteger(p[0]) AS g, p[1] AS c RETURN g, count(c), sum(c), avg(c), min(c), max(c)",
                              {{"1", "2", "PT3S", "PT1.5S", "PT1S", "PT2S"},
                               {"2", "1", "PT1M", "PT1M", "PT1M", "PT1M"},
                               {"3", "0", "0", "null", "null", "null"}});
}

TEST_F(GroupAggregateStateTest, reducesDistinctCells) {
    expectRowsInEveryChunking("UNWIND [[1, 2], [1, 2], [1, 3], [2, 2.5], [2, 2.5], [2, 1]] AS p "
                              "WITH toInteger(p[0]) AS g, p[1] AS c "
                              "RETURN g, count(DISTINCT c), sum(DISTINCT c), avg(DISTINCT c), stDevP(DISTINCT c)",
                              {{"1", "2", "5", "2.500000", "0.500000"},
                               {"2", "2", "3.500000", "1.750000", "0.750000"}});
}

TEST_F(GroupAggregateStateTest, rejectsADurationCellAfterAnIntegerOne) {
    expectError("UNWIND [[1, 1], [1, duration({seconds: 1})]] AS p RETURN toInteger(p[0]) AS g, sum(p[1])",
                "sum() and avg() cannot add 'Duration' to a sum of 'Integer'");
}

TEST_F(GroupAggregateStateTest, rejectsAFloatCellAfterADurationOne) {
    expectError("UNWIND [[1, duration({seconds: 1})], [1, 2.5]] AS p RETURN toInteger(p[0]) AS g, avg(p[1])",
                "sum() and avg() cannot add 'Double' to a sum of 'Duration'");
}

TEST_F(GroupAggregateStateTest, rejectsAStringCell) {
    expectError("UNWIND [[1, 1], [1, 'x']] AS p RETURN toInteger(p[0]) AS g, sum(p[1])",
                "sum() and avg() read numbers or durations, not 'String'");
    expectError("UNWIND [[1, 1], [1, 'x']] AS p RETURN toInteger(p[0]) AS g, stDev(p[1])",
                "stDev() and stDevP() read numbers, not 'String'");
}

// A 2-byte state ahead of the 16-byte aligned ones of durations and cell sums puts each at an
// offset its alignment rounds up
TEST_F(GroupAggregateStateTest, reducesManyAggregatesOfOneGroup) {
    expectRowsInEveryChunking("UNWIND range(1, 6) AS x "
                              "WITH x % 2 AS g, x % 3 = 0 AS b, duration({seconds: x}) AS d, [x, 'a'][0] AS c "
                              "RETURN g, min(b), sum(d), max(b), sum(c), count(*), avg(d), avg(c)",
                              {{"0", "false", "PT12S", "true", "12", "3", "PT4S", "4.000000"},
                               {"1", "false", "PT9S", "true", "9", "3", "PT3S", "3.000000"}});
}

TEST_F(GroupAggregateStateTest, growsTheStatesOverHundredsOfGroups) {
    expectRowsInEveryChunking("UNWIND range(1, 1000) AS x WITH x % 300 AS g, x "
                              "WITH g, sum(x) AS s, avg(x) AS a, count(x) AS n, stDevP(x) AS spread "
                              "RETURN count(g), sum(s), sum(n), min(n), max(n), min(a), max(a), min(spread), max(spread)",
                              {{"300", "500500", "1000", "3", "4", "401.000000", "600.000000", "244.948974", "335.410197"}});
}

TEST_F(GroupAggregateStateTest, growsTheCellStatesOverHundredsOfGroups) {
    expectRowsInEveryChunking("UNWIND range(1, 1000) AS x WITH x % 300 AS g, [x, 'a'][0] AS c "
                              "WITH g, sum(c) AS s, avg(c) AS a, count(*) AS n, max(c) AS m "
                              "RETURN count(g), sum(s), sum(n), min(n), max(n), min(a), max(a), min(m), max(m)",
                              {{"300", "500500", "1000", "3", "4", "401.000000", "600.000000", "701", "1000"}});
}

// Every row replaces its group's extremes, so the store compacts several times on the way
TEST_F(GroupAggregateStateTest, keepsExtremeStringsAcrossCompactions) {
    const Rows expected = {{"0", "80001", "99993"},
                           {"1", "80000", "99999"},
                           {"2", "80006", "99998"},
                           {"3", "80005", "99997"},
                           {"4", "80004", "99996"},
                           {"5", "80003", "99995"},
                           {"6", "80002", "99994"}};

    expectRows("UNWIND range(1, 20000) AS x WITH x % 7 AS g, toString(100000 - x) AS s RETURN g, min(s), max(s)",
               expected);
    expectRows("UNWIND range(1, 20000) AS x WITH x % 7 AS g, [toString(100000 - x), 1][0] AS s RETURN g, min(s), max(s)",
               expected);
}

// A kept extreme is held inline whatever its type, and a string, list or map one keeps the store's
// copy of what it views, across the compactions of the store
TEST_F(GroupAggregateStateTest, keepsTheExtremesOfCellsOfEveryType) {
    expectRowsInEveryChunking("UNWIND [[1, 3], [1, 2.5], [2, 'b'], [2, 'a'], [3, true], [3, false], [4, [2]], [4, [1, 5]], "
                              "[5, {a: 2}], [5, {a: 1}], [6, duration({seconds: 2})], [6, duration({seconds: 1})], "
                              "[7, null], [7, datetime('2020-01-02T00:00:00Z')], [7, datetime('2020-01-01T00:00:00Z')], "
                              "[8, point({x: 2, y: 0})], [8, point({x: 1, y: 5})]] AS p "
                              "WITH toInteger(p[0]) AS g, p[1] AS c "
                              "RETURN g, min(c), max(c)",
                              {{"1", "2.500000", "3"},
                               {"2", "a", "b"},
                               {"3", "false", "true"},
                               {"4", "[1, 5]", "[2]"},
                               {"5", "{a: 1}", "{a: 2}"},
                               {"6", "PT1S", "PT2S"},
                               {"7", "2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z"},
                               {"8", "point({x: 1.0, y: 5.0, crs: 'cartesian'})", "point({x: 2.0, y: 0.0, crs: 'cartesian'})"}});
    expectRows("UNWIND range(1, 20000) AS x WITH x % 7 AS g, [[toString(100000 - x)], 1][0] AS l "
               "RETURN g, min(l), max(l)",
               {{"0", "[80001]", "[99993]"},
                {"1", "[80000]", "[99999]"},
                {"2", "[80006]", "[99998]"},
                {"3", "[80005]", "[99997]"},
                {"4", "[80004]", "[99996]"},
                {"5", "[80003]", "[99995]"},
                {"6", "[80002]", "[99994]"}});
}

TEST_F(GroupAggregateStateTest, reducesBesideACollect) {
    expectRowsInEveryChunking("UNWIND range(1, 9) AS x WITH x % 3 AS g, x, [x, 'a'][0] AS c "
                              "RETURN g, collect(x), sum(c), avg(c), max(c)",
                              {{"0", "[3, 6, 9]", "18", "6.000000", "9"},
                               {"1", "[1, 4, 7]", "12", "4.000000", "7"},
                               {"2", "[2, 5, 8]", "15", "5.000000", "8"}});
}

TEST_F(GroupAggregateStateTest, reducesBesideAPercentile) {
    expectRowsInEveryChunking("UNWIND range(1, 12) AS x RETURN x % 3 AS g, percentileCont(x, 0.5), sum(x)",
                              {{"0", "7.500000", "30"},
                               {"1", "5.500000", "22"},
                               {"2", "6.500000", "26"}});
}
