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

class DistinctHashTableTest : public WriteQueryTest {
protected:
    void expectRowsInOrderInEveryChunking(std::string_view query, const Rows& expected) {
        for (const size_t chunkSize : {size_t {2}, size_t {7}, ChunkConfig::CHUNK_SIZE}) {
            _interpreter->setChunkSize(chunkSize);
            expectRowsInOrder(query, expected);
        }

        _interpreter->setChunkSize(ChunkConfig::CHUNK_SIZE);
    }

    // The hash table the query's DISTINCT at function scope keys its rows with
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
            if (stmt.getFunction() == &NLExecutor::runDistinctReset) {
                const NLDistinctResetData* reset = static_cast<const NLDistinctResetData*>(stmt.getData());
                EXPECT_EQ(reset->getState()->hashTable().getKind(), expected) << "query: " << query;
                return;
            }
        }

        FAIL() << "query: " << query << "\nhas no DISTINCT at function scope";
    }
};

TEST_F(DistinctHashTableTest, choosesAHashTableForEachKeyShape) {
    expectHashTable("MATCH (n)-->(m) RETURN DISTINCT n", NLGroupHashTableKind::Fixed64);
    expectHashTable("MATCH (n) RETURN DISTINCT n.age", NLGroupHashTableKind::NullableFixed64);
    expectHashTable("MATCH (n)-->(m) RETURN DISTINCT n, m", NLGroupHashTableKind::Fixed128);
    expectHashTable("MATCH (a)-->(b)-->(c) RETURN DISTINCT a, b, c", NLGroupHashTableKind::Fixed256);
    expectHashTable("MATCH (n) RETURN DISTINCT n.name", NLGroupHashTableKind::NullableString);
    expectHashTable("MATCH (n) RETURN DISTINCT n.name, n.age", NLGroupHashTableKind::Serialized);
    expectHashTable("MATCH (n) RETURN DISTINCT labels(n)", NLGroupHashTableKind::Serialized);
}

TEST_F(DistinctHashTableTest, keepsTheFirstRowOfEachKey) {
    expectRowsInOrderInEveryChunking("UNWIND range(0, 20) AS x RETURN DISTINCT (x * 2) % 5 AS g",
                                     {{"0"}, {"2"}, {"4"}, {"1"}, {"3"}});
    expectRowsInOrderInEveryChunking("UNWIND [0, null, 0, null, 1, null] AS x RETURN DISTINCT x",
                                     {{"0"}, {"null"}, {"1"}});
    expectRowsInOrderInEveryChunking("UNWIND [0.0, -0.0, 1.5, -1.5, 1.5, -0.0] AS x RETURN DISTINCT x",
                                     {{"0.000000"}, {"1.500000"}, {"-1.500000"}});
    expectRowsInOrderInEveryChunking("UNWIND ['b', null, 'a', '', null, 'b', '', 'ab', 'a'] AS s RETURN DISTINCT s",
                                     {{"b"}, {"null"}, {"a"}, {""}, {"ab"}});
    expectRowsInOrderInEveryChunking("UNWIND [[null, 0], [0, null], [0, 0], [null, null], [0, null], [null, 0]] AS p "
                                     "RETURN DISTINCT toInteger(p[0]) AS a, toInteger(p[1]) AS b",
                                     {{"null", "0"}, {"0", "null"}, {"0", "0"}, {"null", "null"}});
    expectRowsInOrderInEveryChunking("UNWIND [1, 1.0, 'a', 2, 'a', true, null, 2.5, null] AS c RETURN DISTINCT c",
                                     {{"1"}, {"a"}, {"2"}, {"true"}, {"null"}, {"2.500000"}});
    expectRowsInOrderInEveryChunking("UNWIND [[1], [1], [1, 2], [], [], [null], [null]] AS l RETURN DISTINCT l",
                                     {{"[1]"}, {"[1, 2]"}, {"[]"}, {"[null]"}});
}

// Every branch of a union filters into the one seen-set, through the one hash table
TEST_F(DistinctHashTableTest, keysTheBranchesOfAUnionInOneTable) {
    for (const size_t chunkSize : {size_t {2}, size_t {7}, ChunkConfig::CHUNK_SIZE}) {
        _interpreter->setChunkSize(chunkSize);

        expectRows("UNWIND [1, 2, 2, null] AS x RETURN x AS v UNION UNWIND [2, 3, null, 3] AS y RETURN y AS v",
                   {{"1"}, {"2"}, {"3"}, {"null"}});
        expectRows("UNWIND ['a', 'b', 'b'] AS x RETURN x AS v UNION UNWIND ['b', 'c', 'c'] AS y RETURN y AS v",
                   {{"a"}, {"b"}, {"c"}});
        expectRows("UNWIND [1, 'a', 1.0] AS x RETURN x AS v UNION UNWIND ['a', 2, 1] AS y RETURN y AS v",
                   {{"1"}, {"a"}, {"2"}});
        expectRows("MATCH (n:Person)-->(m) RETURN n.name AS v UNION MATCH (n)-->(m:Person) RETURN m.name AS v",
                   {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Cyrus"}, {"Doruk"}, {"Suhas"}});
    }

    _interpreter->setChunkSize(ChunkConfig::CHUNK_SIZE);
}

TEST_F(DistinctHashTableTest, keepsTheRowsOfThousandsOfKeys) {
    for (const size_t chunkSize : {size_t {2}, size_t {7}, ChunkConfig::CHUNK_SIZE}) {
        _interpreter->setChunkSize(chunkSize);

        expectRows("UNWIND range(1, 30000) AS x WITH DISTINCT x % 7919 AS g RETURN count(g), sum(g)",
                   {{"7919", "31351321"}});
        expectRows("UNWIND range(1, 30000) AS x WITH DISTINCT toString(x % 7919) AS g RETURN count(g)",
                   {{"7919"}});
        expectRows("UNWIND range(1, 30000) AS x WITH DISTINCT x % 89 AS a, x % 97 AS b RETURN count(*)",
                   {{"8633"}});
        expectRows("UNWIND range(1, 30000) AS x WITH DISTINCT [x % 7919] AS g RETURN count(*)",
                   {{"7919"}});
    }

    _interpreter->setChunkSize(ChunkConfig::CHUNK_SIZE);
}
