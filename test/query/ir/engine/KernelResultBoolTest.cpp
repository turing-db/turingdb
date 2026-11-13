#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRException.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

// MATCH (n) RETURN n IS NOT NULL under AND, OR, = and NOT, with AND and NOT declared
// both !storage.bool and i1
constexpr const char* KERNEL_BOOLEANS_PROGRAM = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %known = nl.is_not_null %a : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.bool>
    %both = nl.and %known, %known : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>) -> !nl.chunk<!storage.bool>
    %bothI1 = nl.and %known, %known : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>) -> !nl.chunk<i1>
    %either = nl.or %known, %known : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>) -> !nl.chunk<!storage.bool>
    %same = nl.eq %known, %known : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>) -> !nl.chunk<!storage.bool>
    %absent = nl.not %known : (!nl.chunk<!storage.bool>) -> !nl.chunk<!storage.bool>
    %absentI1 = nl.not %known : (!nl.chunk<!storage.bool>) -> !nl.chunk<i1>
    %n1 = nl.to_nullable %both : (!nl.chunk<!storage.bool>) -> !nl.chunk<!storage.nullable<i1>>
    %n2 = nl.to_nullable %bothI1 : (!nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
    %n3 = nl.to_nullable %either : (!nl.chunk<!storage.bool>) -> !nl.chunk<!storage.nullable<i1>>
    %n4 = nl.to_nullable %same : (!nl.chunk<!storage.bool>) -> !nl.chunk<!storage.nullable<i1>>
    %n5 = nl.to_nullable %absent : (!nl.chunk<!storage.bool>) -> !nl.chunk<!storage.nullable<i1>>
    %n6 = nl.to_nullable %absentI1 : (!nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
    nl.output(%n1, %n2, %n3, %n4, %n5, %n6) : !nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>
  }
  func.return
}
)mlir";

// MATCH (n) WHERE 1 = 2 RETURN n, with the comparison declared a chunk of integers
constexpr const char* COMPARISON_DECLARED_INTEGERS_PROGRAM = R"mlir(
func.func @main() {
  %k1 = nl.constant(1 : i64)
  %k2 = nl.constant(2 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %same = nl.eq %k1, %k2 : (!nl.chunk<i64>, !nl.chunk<i64>) -> !nl.chunk<i64>
    nl.output(%same) : !nl.chunk<i64>
  }
  func.return
}
)mlir";

constexpr size_t SIMPLEDB_NODE_COUNT = 18;

}

class KernelResultBoolTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void runProgram(const char* programText, NLOutputSink& sink) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        LocalMemory memory;
        NLInterpreter interpreter(*module, &reader.getView(), &sink, &memory);
        interpreter.run();
    }

    std::unique_ptr<Graph> _graph;
};

TEST_F(KernelResultBoolTest, readsAKernelBooleanDeclaredEitherBooleanType) {
    RowSink sink;
    runProgram(KERNEL_BOOLEANS_PROGRAM, sink);

    const Rows expected(SIMPLEDB_NODE_COUNT, {"true", "true", "true", "true", "false", "false"});
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(KernelResultBoolTest, rejectsABooleanKernelDeclaredIntegers) {
    RowSink sink;
    try {
        runProgram(COMPARISON_DECLARED_INTEGERS_PROGRAM, sink);
        ADD_FAILURE() << "The program ran instead of being rejected";
    } catch (const IRException& exception) {
        const std::string_view message = exception.what();
        EXPECT_NE(message.find("nl.eq declares a !nl.chunk<i64> result"), std::string_view::npos) << message;
    }

    EXPECT_TRUE(sink.rows().empty());
}

// The same predicates written in Cypher. Of the 8 people, Remy and Adam have an age and a KNOWS_WELL edge out, and their
// edges out number Remy 4, Adam 3, Maxime 2, Luc 2, Martina 1, Suhas 2, Cyrus 2, Doruk 1.
class KernelResultBoolQueryTest : public WriteQueryTest {
};

TEST_F(KernelResultBoolQueryTest, readsANullTestOutOfABodyRunOncePerRow) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "CALL (f) { WITH count(*) AS c RETURN f IS NULL AS isnull, c } "
               "RETURN p.name, isnull, c",
               {
                   {"Remy", "false", "1"},
                   {"Adam", "false", "1"},
                   {"Maxime", "true", "1"},
                   {"Luc", "true", "1"},
                   {"Martina", "true", "1"},
                   {"Suhas", "true", "1"},
                   {"Cyrus", "true", "1"},
                   {"Doruk", "true", "1"},
               });
}

TEST_F(KernelResultBoolQueryTest, readsANotNullTestOutOfABodyRunOncePerRow) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "CALL (f) { WITH count(*) AS c RETURN f IS NOT NULL AS known, c > 0 AS some } "
               "RETURN p.name, known, some",
               {
                   {"Remy", "true", "true"},
                   {"Adam", "true", "true"},
                   {"Maxime", "false", "true"},
                   {"Luc", "false", "true"},
                   {"Martina", "false", "true"},
                   {"Suhas", "false", "true"},
                   {"Cyrus", "false", "true"},
                   {"Doruk", "false", "true"},
               });
}

TEST_F(KernelResultBoolQueryTest, testsAPropertyForNull) {
    expectRows("MATCH (p:Person) RETURN p.name, p.age IS NULL, p.age IS NOT NULL",
               {
                   {"Remy", "false", "true"},
                   {"Adam", "false", "true"},
                   {"Maxime", "true", "false"},
                   {"Luc", "true", "false"},
                   {"Martina", "true", "false"},
                   {"Suhas", "true", "false"},
                   {"Cyrus", "true", "false"},
                   {"Doruk", "true", "false"},
               });
}

TEST_F(KernelResultBoolQueryTest, comparesACount) {
    expectRows("MATCH (p:Person) WITH p, count { (p)-->() } AS c "
               "RETURN p.name, c = 2, c <> 2, c < 2, c <= 2, c > 2, c >= 2",
               {
                   {"Remy", "false", "true", "false", "false", "true", "true"},
                   {"Adam", "false", "true", "false", "false", "true", "true"},
                   {"Maxime", "true", "false", "false", "true", "false", "true"},
                   {"Luc", "true", "false", "false", "true", "false", "true"},
                   {"Martina", "false", "true", "true", "true", "false", "false"},
                   {"Suhas", "true", "false", "false", "true", "false", "true"},
                   {"Cyrus", "true", "false", "false", "true", "false", "true"},
                   {"Doruk", "false", "true", "true", "true", "false", "false"},
               });
}

TEST_F(KernelResultBoolQueryTest, connectsTwoBooleans) {
    expectRows("MATCH (p:Person) WITH p, count { (p)-->() } AS c "
               "RETURN p.name, p.age IS NULL AND c = 2, p.age IS NULL OR c > 3, p.age IS NULL XOR c = 1",
               {
                   {"Remy", "false", "true", "false"},
                   {"Adam", "false", "false", "false"},
                   {"Maxime", "true", "true", "true"},
                   {"Luc", "true", "true", "true"},
                   {"Martina", "false", "true", "false"},
                   {"Suhas", "true", "true", "true"},
                   {"Cyrus", "true", "true", "true"},
                   {"Doruk", "false", "true", "false"},
               });
}

TEST_F(KernelResultBoolQueryTest, negatesABoolean) {
    expectRows("MATCH (p:Person) WITH p, count { (p)-->() } AS c "
               "RETURN p.name, NOT (p.age IS NULL), NOT (c > 2)",
               {
                   {"Remy", "true", "false"},
                   {"Adam", "true", "false"},
                   {"Maxime", "false", "true"},
                   {"Luc", "false", "true"},
                   {"Martina", "false", "true"},
                   {"Suhas", "false", "true"},
                   {"Cyrus", "false", "true"},
                   {"Doruk", "false", "true"},
               });
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
