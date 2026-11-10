#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"

#include "Graph.h"
#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureManager.h"
#include "ProcedureNamespace.h"
#include "ProcedureState.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "IRTestRows.h"
#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using BoolColumn = ColumnVector<types::Bool::Primitive>;

struct FlagsData : public ProcedureData {};

// test.flags yields the four flags true, false, false, true as one chunk of plain booleans
void flagsExecuteImpl(ProcedureState* procedureState) {
    FlagsData& data = procedureState->data<FlagsData>();

    BoolColumn* flags = static_cast<BoolColumn*>(data.getReturnColumn(0));
    flags->clear();
    flags->push_back(true);
    flags->push_back(false);
    flags->push_back(false);
    flags->push_back(true);

    procedureState->finish();
}

void flagsExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        flagsExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* flagsAlloc() {
    return new FlagsData();
}

void flagsDealloc(ProcedureData* data) {
    delete data;
}

}

// A boolean chunk is held in the column its type names: !storage.bool in a mask, i1 in a
// column of plain booleans, !storage.nullable<i1> in a nullable one. Every kernel reading
// one is picked from that type, and the translator rejects a truth value held in a column
// of another class and a mask read through a kernel of plain booleans. Of the 8 people,
// Remy and Adam have an age.
class BooleanColumnClassTest : public TuringTest {
protected:
    void initialize() override {
        _procedures.init();

        ProcedureNamespace* testNamespace = _procedures.createNamespace("test");

        Procedure* flagsProcedure = new Procedure("flags");
        flagsProcedure->setAllocCallback(&flagsAlloc);
        flagsProcedure->setDeallocCallback(&flagsDealloc);
        flagsProcedure->setExecuteCallback(&flagsExecute);
        flagsProcedure->addReturnValue("flag", ProcedureType::BOOL);
        testNamespace->addProcedure(flagsProcedure);

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void runQuery(const char* queryText, NLOutputSink& sink) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(&_procedures, queryText);

        CypherParser parser(&ast);
        parser.parse(queryText);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);

        LocalMemory memory;

        ProcedureContext procedureContext;
        procedureContext.setGraphView(&view);
        procedureContext.setProcedures(&_procedures);
        procedureContext.setChunkSize(ChunkConfig::CHUNK_SIZE);
        procedureContext.setListBuffer(&memory.listBuffer());
        procedureContext.setStringBuffer(&memory.stringBuffer());

        DBDialectInterpreter interpreter(moduleOp,
                                         &view,
                                         &sink,
                                         &memory,
                                         ChunkConfig::CHUNK_SIZE,
                                         nullptr,
                                         nullptr,
                                         &procedureContext);
        interpreter.run();
    }

    void expectRows(const char* query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    ProcedureManager _procedures;
    std::unique_ptr<Graph> _graph;
};

TEST_F(BooleanColumnClassTest, negatesTheYieldedFlags) {
    expectRows("CALL test.flags() YIELD flag RETURN flag, NOT flag",
               {
                   {"true", "false"},
                   {"false", "true"},
                   {"false", "true"},
                   {"true", "false"},
               });
}

TEST_F(BooleanColumnClassTest, filtersOnTheYieldedFlags) {
    expectRows("CALL test.flags() YIELD flag WITH flag WHERE flag RETURN flag", {{"true"}, {"true"}});
    expectRows("CALL test.flags() YIELD flag WITH flag WHERE NOT flag RETURN flag", {{"false"}, {"false"}});
}

TEST_F(BooleanColumnClassTest, filtersOnTheYieldedFlagsMetWithAMask) {
    expectRows("CALL test.flags() YIELD flag MATCH (p:Person) WHERE flag AND p.age IS NULL RETURN count(*)", {{"12"}});
    expectRows("CALL test.flags() YIELD flag MATCH (p:Person) WHERE flag OR p.age IS NULL RETURN count(*)", {{"28"}});
    expectRows("CALL test.flags() YIELD flag MATCH (p:Person) WHERE flag XOR p.age IS NULL RETURN count(*)", {{"16"}});
}

TEST_F(BooleanColumnClassTest, meetsTheYieldedFlagsWithAMask) {
    expectRows("CALL test.flags() YIELD flag MATCH (p:Person {name: 'Remy'}) "
               "RETURN flag, flag AND p.age IS NULL, flag OR p.age IS NULL, flag XOR p.age IS NOT NULL",
               {
                   {"true", "false", "true", "false"},
                   {"false", "false", "false", "true"},
                   {"false", "false", "false", "true"},
                   {"true", "false", "true", "false"},
               });
}

TEST_F(BooleanColumnClassTest, takesACaseBranchOnTheYieldedFlags) {
    expectRows("CALL test.flags() YIELD flag RETURN flag, CASE WHEN flag THEN 'yes' ELSE 'no' END",
               {
                   {"true", "yes"},
                   {"false", "no"},
                   {"false", "no"},
                   {"true", "yes"},
               });

    expectRows("CALL test.flags() YIELD flag MATCH (p:Person {name: 'Remy'}) "
               "RETURN flag, CASE WHEN NOT flag THEN p.name WHEN flag THEN toUpper(p.name) END",
               {
                   {"true", "REMY"},
                   {"false", "Remy"},
                   {"false", "Remy"},
                   {"true", "REMY"},
               });
}

TEST_F(BooleanColumnClassTest, filtersOnTheYieldedFlagsInASubqueryBody) {
    expectRows("CALL test.flags() YIELD flag RETURN flag, COUNT { MATCH (p:Person) WHERE flag } AS c",
               {
                   {"true", "8"},
                   {"false", "0"},
                   {"false", "0"},
                   {"true", "8"},
               });

    expectRows("CALL test.flags() YIELD flag RETURN flag, EXISTS { MATCH (p:Person) WHERE NOT flag AND p.age IS NULL }",
               {
                   {"true", "false"},
                   {"false", "true"},
                   {"false", "true"},
                   {"true", "false"},
               });
}

// The 8 people start 25 paths of 1 or 2 hops
TEST_F(BooleanColumnClassTest, filtersTheHopsOfAPathOnTheYieldedFlags) {
    expectRows("CALL test.flags() YIELD flag MATCH (p:Person)-[e*1..2]->(q) WHERE all(r IN e WHERE flag) RETURN count(*)",
               {{"50"}});
}

TEST_F(BooleanColumnClassTest, readsTheYieldedFlagsInAListPredicate) {
    expectRows("CALL test.flags() YIELD flag RETURN flag, all(x IN [1, 2] WHERE flag), any(x IN [1, 2] WHERE NOT flag)",
               {
                   {"true", "true", "false"},
                   {"false", "false", "true"},
                   {"false", "false", "true"},
                   {"true", "true", "false"},
               });
}

TEST_F(BooleanColumnClassTest, readsTheYieldedFlagsInAnAllReduce) {
    expectRows("CALL test.flags() YIELD flag RETURN flag, allReduce(acc = 0, x IN [1, 2] | acc + x, flag)",
               {
                   {"true", "true"},
                   {"false", "false"},
                   {"false", "false"},
                   {"true", "true"},
               });
}

TEST_F(BooleanColumnClassTest, readsBooleanConstants) {
    expectRows("RETURN NOT true, NOT false", {{"false", "true"}});
    expectRows("MATCH (p:Person) WHERE NOT false RETURN count(*)", {{"8"}});
    expectRows("MATCH (p:Person) WITH p, true AS t WHERE t AND p.age IS NULL RETURN count(*)", {{"6"}});
    expectRows("CALL test.flags() YIELD flag WITH flag, true AS t WHERE t AND flag RETURN count(*)", {{"2"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN CASE WHEN false THEN 'yes' ELSE 'no' END", {{"no"}});
}

TEST_F(BooleanColumnClassTest, readsBooleanListElements) {
    expectRows("RETURN NOT [true, false][0]", {{"false"}});
    expectRows("UNWIND [0, 1] AS i WITH i, [true, false][i] AS b WHERE b RETURN i", {{"0"}});
    expectRows("UNWIND [0, 1] AS i RETURN i, NOT [true, false][i]", {{"0", "false"}, {"1", "true"}});
    expectRows("UNWIND [0, 1] AS i RETURN i, CASE WHEN [true, false][i] THEN 'yes' ELSE 'no' END",
               {{"0", "yes"}, {"1", "no"}});
    expectRows("MATCH (p:Person) WHERE [p.age IS NULL, true][0] RETURN count(*)", {{"6"}});
}

TEST_F(BooleanColumnClassTest, readsAMaskGroupingKey) {
    expectRows("MATCH (p:Person) WITH p.age IS NULL AS k, count(*) AS c WHERE k RETURN c", {{"6"}});
    expectRows("MATCH (p:Person) WITH p.age IS NULL AS k, count(*) AS c RETURN k, c, NOT k",
               {{"false", "2", "true"}, {"true", "6", "false"}});
    expectRows("MATCH (p:Person) WITH p.age IS NULL AS k, collect(p.name) AS c WHERE k RETURN size(c)", {{"6"}});
    expectRows("MATCH (p:Person) WITH p.age IS NULL AS k, collect(p.name) AS c UNWIND c AS x WITH k, x WHERE NOT k RETURN x",
               {{"Adam"}, {"Remy"}});
}

TEST_F(BooleanColumnClassTest, countsTheDistinctValuesOfAMaskPerGroup) {
    expectRows("MATCH (p:Person) RETURN p.name IS NOT NULL AS k, count(DISTINCT p.age IS NULL)", {{"true", "2"}});
    expectRows("MATCH (n) RETURN n:Person AS k, count(DISTINCT n.age IS NULL)", {{"true", "2"}, {"false", "1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
