#include <gtest/gtest.h>

#include <stddef.h>

#include <string_view>
#include <vector>

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

class GroupDistinctTallyTest : public WriteQueryTest {
protected:
    void expectRowsInEveryChunking(std::string_view query, const Rows& expected) {
        for (const size_t chunkSize : {size_t {2}, size_t {7}, ChunkConfig::CHUNK_SIZE}) {
            _interpreter->setChunkSize(chunkSize);
            expectRows(query, expected);
        }

        _interpreter->setChunkSize(ChunkConfig::CHUNK_SIZE);
    }

    // The kinds of the hash tables that number the values of the DISTINCT aggregates and
    // collects of the query's grouping at function scope
    void expectTallyTables(std::string_view query, const std::vector<NLGroupHashTableKind>& expected) {
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

        std::vector<NLGroupHashTableKind> kinds;

        const NLStmtContainer* stmts = program.getStmts();
        for (const NLFunctionDescriptor& stmt : stmts->stmts()) {
            if (stmt.getFunction() == &NLExecutor::runGroupAggregateReset) {
                const NLGroupAggregateResetData* reset = static_cast<const NLGroupAggregateResetData*>(stmt.getData());
                for (const NLGroupAggregates::Aggregate& aggregate : reset->getState()->aggregates().getAggregates()) {
                    if (aggregate._distinct) {
                        kinds.push_back(aggregate._distinct->getValueKind());
                    }
                }
            } else if (stmt.getFunction() == &NLExecutor::runCollectReset) {
                const NLCollectResetData* reset = static_cast<const NLCollectResetData*>(stmt.getData());
                NLCollectState* state = reset->getState();

                for (const NLCollectState::ValueColumn& value : state->valueColumns()) {
                    if (value._distinct) {
                        kinds.push_back(value._distinct->getValueKind());
                    }
                }

                for (const NLGroupAggregates::Aggregate& aggregate : state->aggregates().getAggregates()) {
                    if (aggregate._distinct) {
                        kinds.push_back(aggregate._distinct->getValueKind());
                    }
                }
            }
        }

        EXPECT_EQ(kinds, expected) << "query: " << query;
    }
};

TEST_F(GroupDistinctTallyTest, choosesAHashTableForEachValueShape) {
    expectTallyTables("MATCH (p:Person)-->(i) RETURN p.isFrench, count(DISTINCT i), collect(DISTINCT i)",
                      {NLGroupHashTableKind::Fixed64, NLGroupHashTableKind::Fixed64});
    expectTallyTables("MATCH (n) RETURN n.isFrench, count(DISTINCT n.hasPhD)", {NLGroupHashTableKind::NullableFixed64});
    expectTallyTables("MATCH (n) RETURN n.isFrench, count(DISTINCT n.age), sum(DISTINCT n.age)",
                      {NLGroupHashTableKind::NullableFixed64, NLGroupHashTableKind::NullableFixed64});
    expectTallyTables("MATCH (n) RETURN n.isFrench, count(DISTINCT n.name)", {NLGroupHashTableKind::NullableString});
    expectTallyTables("UNWIND [1, 'a'] AS c RETURN c, count(DISTINCT c)", {NLGroupHashTableKind::Serialized});
    expectTallyTables("UNWIND range(1, 3) AS x RETURN collect(DISTINCT x)", {NLGroupHashTableKind::NullableFixed64});
}

TEST_F(GroupDistinctTallyTest, chargesEachValueOfAGroupOnce) {
    expectRowsInEveryChunking("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
                              "WITH p.isFrench AS french, count(DISTINCT i) AS n, collect(DISTINCT i) AS is "
                              "RETURN french, n, size(is)",
                              {{"true", "7", "7"}, {"false", "4", "4"}});
    expectRowsInEveryChunking("UNWIND [[1, 0], [1, null], [1, 0], [2, null], [2, null], [1, 2], [3, 2]] AS p "
                              "WITH toInteger(p[0]) AS g, toInteger(p[1]) AS v "
                              "RETURN g, count(DISTINCT v), sum(DISTINCT v), collect(DISTINCT v)",
                              {{"1", "2", "2", "[0, 2]"}, {"2", "0", "0", "[]"}, {"3", "1", "2", "[2]"}});
    expectRowsInEveryChunking("UNWIND [[1, 0.0], [1, -0.0], [1, 1.5], [2, 1.5]] AS p "
                              "WITH toInteger(p[0]) AS g, toFloat(p[1]) AS v RETURN g, count(DISTINCT v), avg(DISTINCT v)",
                              {{"1", "2", "0.750000"}, {"2", "1", "1.500000"}});
    expectRowsInEveryChunking("UNWIND [[1, 2], [1, 2], [1, 4], [2, 3]] AS p "
                              "WITH toInteger(p[0]) AS g, toInteger(p[1]) AS v RETURN g, stDevP(DISTINCT v)",
                              {{"1", "1.000000"}, {"2", "0.000000"}});
    expectRowsInEveryChunking("UNWIND range(1, 9) AS x WITH x % 3 AS g, duration({seconds: x % 2}) AS d "
                              "RETURN g, count(DISTINCT d), sum(DISTINCT d)",
                              {{"0", "2", "PT1S"}, {"1", "2", "PT1S"}, {"2", "2", "PT1S"}});
}

TEST_F(GroupDistinctTallyTest, chargesCellsListsAndStringsByValue) {
    expectRowsInEveryChunking("UNWIND [[1, 1], [1, 1.0], [1, 'a'], [2, 1], [1, 'a'], [2, 2], [2, null]] AS p "
                              "WITH toInteger(p[0]) AS g, p[1] AS c RETURN g, count(DISTINCT c)",
                              {{"1", "2"}, {"2", "2"}});
    expectRowsInEveryChunking("UNWIND [[1, 1], [1, 1.0], [1, 2], [2, 2.5], [2, 2.5]] AS p "
                              "WITH toInteger(p[0]) AS g, p[1] AS c RETURN g, sum(DISTINCT c), avg(DISTINCT c)",
                              {{"1", "3", "1.500000"}, {"2", "2.500000", "2.500000"}});
    expectRowsInEveryChunking("UNWIND [[1, [1]], [1, [1]], [1, [2]], [2, [1]], [2, []], [2, []]] AS p "
                              "WITH toInteger(p[0]) AS g, p[1] AS l RETURN g, count(DISTINCT l), size(collect(DISTINCT l))",
                              {{"1", "2", "2"}, {"2", "2", "2"}});
    expectRowsInEveryChunking("UNWIND ['b', 'a', 'b', null, 'cc', 'a', 'cc'] AS s WITH s, size(s) AS n "
                              "RETURN n, count(DISTINCT s), collect(DISTINCT s)",
                              {{"1", "2", "[b, a]"}, {"2", "1", "[cc]"}, {"null", "0", "[]"}});
}

TEST_F(GroupDistinctTallyTest, collectsDistinctValuesWithoutAKey) {
    expectRowsInEveryChunking("UNWIND [3, 1, 3, null, 2, 1] AS x RETURN collect(DISTINCT x)", {{"[3, 1, 2]"}});
    expectRowsInEveryChunking("UNWIND ['b', 'a', 'b'] AS x RETURN collect(DISTINCT x)", {{"[b, a]"}});
}

// Every group meets every value, so the tallies hold 7 x 100 pairs, most of them found again in
// a later chunk than the one that charged them
TEST_F(GroupDistinctTallyTest, chargesThousandsOfPairs) {
    expectRowsInEveryChunking("UNWIND range(1, 30000) AS x WITH x % 7 AS g, x % 100 AS v "
                              "WITH g, count(DISTINCT v) AS n, sum(DISTINCT v) AS s, size(collect(DISTINCT v)) AS c "
                              "RETURN count(*), min(n), max(n), min(s), max(s), min(c), max(c)",
                              {{"7", "100", "100", "4950", "4950", "100", "100"}});
    expectRowsInEveryChunking("UNWIND range(1, 30000) AS x WITH x % 7 AS g, toString(x % 100) AS v "
                              "WITH g, count(DISTINCT v) AS n, size(collect(DISTINCT v)) AS c "
                              "RETURN count(*), min(n), max(n), min(c), max(c)",
                              {{"7", "100", "100", "100", "100"}});
}
