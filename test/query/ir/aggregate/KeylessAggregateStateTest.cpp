#include <gtest/gtest.h>

#include <stddef.h>

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

class KeylessAggregateStateTest : public WriteQueryTest {
protected:
    void expectRowsInEveryChunking(std::string_view query, const Rows& expected) {
        for (const size_t chunkSize : {size_t {2}, size_t {7}, ChunkConfig::CHUNK_SIZE}) {
            _interpreter->setChunkSize(chunkSize);
            expectRows(query, expected);
        }

        _interpreter->setChunkSize(ChunkConfig::CHUNK_SIZE);
    }

    // The values the keyless aggregates of the query copied into their stores and still hold
    // there once it has run
    void keptValueCount(std::string_view query, size_t chunkSize, size_t& count) {
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

        count = 0;
        size_t aggregateCount = 0;

        const NLStmtContainer* stmts = program.getStmts();
        for (const NLFunctionDescriptor& stmt : stmts->stmts()) {
            if (stmt.getFunction() == &NLExecutor::runAggregateReset) {
                const NLAggregateResetData* reset = static_cast<const NLAggregateResetData*>(stmt.getData());
                count += reset->getState()->getValues().getActive().getValueCount();
                aggregateCount++;
            }
        }

        EXPECT_GT(aggregateCount, 0) << "query: " << query << "\nruns no keyless aggregate at function scope";
    }
};

TEST_F(KeylessAggregateStateTest, sumAvgMinAndMaxOfNumberCellsCopyNothing) {
    size_t count = 0;
    keptValueCount("UNWIND range(1, 40) AS x WITH [x, 'a'][0] AS c, [toFloat(x), 'a'][0] AS f "
                   "RETURN sum(c), avg(c), min(c), max(c), sum(f), max(f)",
                   8,
                   count);

    EXPECT_EQ(count, 0);
}

TEST_F(KeylessAggregateStateTest, maxOfStringsKeepsOneCopy) {
    size_t count = 0;
    keptValueCount("UNWIND range(1, 40) AS x WITH toString(100 + x) AS s RETURN max(s)", 8, count);

    EXPECT_EQ(count, 1);
}

TEST_F(KeylessAggregateStateTest, reducesNumbers) {
    expectRowsInEveryChunking("UNWIND [3, null, 1, 4, null, 1, 5] AS x "
                              "RETURN sum(x), avg(x), min(x), max(x), stDev(x), stDevP(x)",
                              {{"14", "2.800000", "1", "5", "1.788854", "1.600000"}});
    expectRowsInEveryChunking("UNWIND [3.5, null, -1.0, 4.25] AS x RETURN sum(x), avg(x), min(x), max(x)",
                              {{"6.750000", "2.250000", "-1.000000", "4.250000"}});
}

TEST_F(KeylessAggregateStateTest, reducesNoRow) {
    expectRows("UNWIND [] AS x RETURN sum(x), avg(x), min(x), max(x), stDev(x)",
               {{"0", "null", "null", "null", "0.000000"}});
    expectRows("UNWIND [null, null] AS x RETURN sum(x), avg(x), min(x), max(x), stDevP(x)",
               {{"0", "null", "null", "null", "0.000000"}});
}

TEST_F(KeylessAggregateStateTest, reducesDurations) {
    expectRowsInEveryChunking("UNWIND range(1, 9) AS x WITH duration({seconds: x}) AS d "
                              "RETURN sum(d), avg(d), min(d), max(d)",
                              {{"PT45S", "PT5S", "PT1S", "PT9S"}});
}

TEST_F(KeylessAggregateStateTest, keepsTheExtremeStrings) {
    expectRowsInEveryChunking("MATCH (n) RETURN min(n.name), max(n.name), min(n.dob), max(n.dob)",
                              {{"Adam", "Travel", "18/01", "28/05"}});
    expectRows("UNWIND range(1, 20000) AS x WITH toString(100000 - x) AS s RETURN min(s), max(s)",
               {{"80000", "99999"}});
}

TEST_F(KeylessAggregateStateTest, reducesCells) {
    expectRowsInEveryChunking("UNWIND range(1, 12) AS x WITH [x, 'a'][0] AS c "
                              "RETURN sum(c), avg(c), min(c), max(c), stDev(c), stDevP(c)",
                              {{"78", "6.500000", "1", "12", "3.605551", "3.452053"}});
    expectRowsInEveryChunking("UNWIND [1, 2, 0.5, null, 3.5] AS c RETURN sum(c), avg(c), min(c), max(c)",
                              {{"7.000000", "1.750000", "0.500000", "3.500000"}});
    expectRowsInEveryChunking("UNWIND [duration({seconds: 2}), null, duration({minutes: 1})] AS c "
                              "RETURN sum(c), avg(c), min(c), max(c)",
                              {{"PT1M2S", "PT31S", "PT2S", "PT1M"}});
    expectRowsInEveryChunking("UNWIND ['b', 1, 'a', [1], {a: 1}, true, null] AS c RETURN min(c), max(c)",
                              {{"{a: 1}", "1"}});
    expectRows("UNWIND range(1, 20000) AS x WITH [toString(100000 - x), 1][0] AS c RETURN min(c), max(c)",
               {{"80000", "99999"}});
}

TEST_F(KeylessAggregateStateTest, reducesNoCell) {
    expectRows("UNWIND [1, 'a', null] AS c WITH c WHERE c IS NULL RETURN sum(c), avg(c), min(c), max(c)",
               {{"0", "null", "null", "null"}});
    expectRows("UNWIND [1, 'a'] AS c WITH c WHERE c IS NULL RETURN sum(c), avg(c), min(c), max(c)",
               {{"0", "null", "null", "null"}});
}

TEST_F(KeylessAggregateStateTest, rejectsMixingDurationsWithNumbers) {
    expectError("UNWIND [1, duration({seconds: 1})] AS c RETURN sum(c)",
                "sum() and avg() cannot add 'Duration' to a sum of 'Integer'");
    expectError("UNWIND [1, 'x'] AS c RETURN stDev(c)", "stDev() and stDevP() read numbers, not 'String'");
}
