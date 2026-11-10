#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <string>
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

// MATCH (n) RETURN n IS NOT NULL AND n IS NOT NULL, with the conjunction of the two masks
// declared a column of plain booleans: its kernel writes a mask
constexpr const char* MASKS_CONNECTED_INTO_PLAIN_BOOLEANS_PROGRAM = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %known = nl.is_not_null %a : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.bool>
    %both = nl.and %known, %known : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>) -> !nl.chunk<i1>
    %n = nl.to_nullable %both : (!nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
    nl.output(%n) : !nl.chunk<!storage.nullable<i1>>
  }
  func.return
}
)mlir";

// The same mask negated, with the negation declared a column of plain booleans
constexpr const char* MASK_NEGATED_INTO_PLAIN_BOOLEANS_PROGRAM = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %known = nl.is_not_null %a : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.bool>
    %absent = nl.not %known : (!nl.chunk<!storage.bool>) -> !nl.chunk<i1>
    %n = nl.to_nullable %absent : (!nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
    nl.output(%n) : !nl.chunk<!storage.nullable<i1>>
  }
  func.return
}
)mlir";

// The mask under AND, OR, = and NOT, each declared a mask
constexpr const char* MASKS_DECLARED_MASKS_PROGRAM = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %known = nl.is_not_null %a : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.bool>
    %both = nl.and %known, %known : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>) -> !nl.chunk<!storage.bool>
    %either = nl.or %known, %known : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>) -> !nl.chunk<!storage.bool>
    %same = nl.eq %known, %known : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>) -> !nl.chunk<!storage.bool>
    %absent = nl.not %known : (!nl.chunk<!storage.bool>) -> !nl.chunk<!storage.bool>
    %n1 = nl.to_nullable %both : (!nl.chunk<!storage.bool>) -> !nl.chunk<!storage.nullable<i1>>
    %n2 = nl.to_nullable %either : (!nl.chunk<!storage.bool>) -> !nl.chunk<!storage.nullable<i1>>
    %n3 = nl.to_nullable %same : (!nl.chunk<!storage.bool>) -> !nl.chunk<!storage.nullable<i1>>
    %n4 = nl.to_nullable %absent : (!nl.chunk<!storage.bool>) -> !nl.chunk<!storage.nullable<i1>>
    nl.output(%n1, %n2, %n3, %n4) : !nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>
  }
  func.return
}
)mlir";

constexpr size_t SIMPLEDB_NODE_COUNT = 18;

}

// A boolean a kernel computes over columns that hold no null is a mask, which the chunk
// type !nl.chunk<!storage.bool> names. !nl.chunk<i1> names a column of plain booleans, so
// the translator rejects a kernel result declared one that is written as a mask.
class KernelResultMaskTest : public TuringTest {
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

    void expectRejected(const char* programText, std::string_view expectedMessage) {
        RowSink sink;
        try {
            runProgram(programText, sink);
            ADD_FAILURE() << "The program ran instead of being rejected";
        } catch (const IRException& exception) {
            const std::string_view message = exception.what();
            EXPECT_NE(message.find(expectedMessage), std::string_view::npos) << message;
        }

        EXPECT_TRUE(sink.rows().empty());
    }

    std::unique_ptr<Graph> _graph;
};

TEST_F(KernelResultMaskTest, masksConnectedIntoPlainBooleansAreRejected) {
    expectRejected(MASKS_CONNECTED_INTO_PLAIN_BOOLEANS_PROGRAM,
                   "nl.and declares a !nl.chunk<i1> result, but its kernel writes a db::ColumnMask");
}

TEST_F(KernelResultMaskTest, maskNegatedIntoPlainBooleansIsRejected) {
    expectRejected(MASK_NEGATED_INTO_PLAIN_BOOLEANS_PROGRAM,
                   "nl.not declares a !nl.chunk<i1> result, but its kernel writes a db::ColumnMask");
}

TEST_F(KernelResultMaskTest, masksDeclaredMasks) {
    RowSink sink;
    runProgram(MASKS_DECLARED_MASKS_PROGRAM, sink);

    const Rows expected(SIMPLEDB_NODE_COUNT, {"true", "true", "true", "false"});
    EXPECT_EQ(sink.rows(), expected);
}

// The same predicates written in Cypher: lowering types each of them as the mask its kernel
// writes. Of the 8 people, Remy and Adam have an age and a KNOWS_WELL edge out, and their
// edges out number Remy 4, Adam 3, Maxime 2, Luc 2, Martina 1, Suhas 2, Cyrus 2, Doruk 1.
class KernelResultMaskQueryTest : public WriteQueryTest {
};

TEST_F(KernelResultMaskQueryTest, readsANullTestOutOfABodyRunOncePerRow) {
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

TEST_F(KernelResultMaskQueryTest, readsANotNullTestOutOfABodyRunOncePerRow) {
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

TEST_F(KernelResultMaskQueryTest, testsAPropertyForNull) {
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

TEST_F(KernelResultMaskQueryTest, comparesACount) {
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

TEST_F(KernelResultMaskQueryTest, connectsTwoMasks) {
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

TEST_F(KernelResultMaskQueryTest, negatesAMask) {
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
