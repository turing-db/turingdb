#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "columns/ColumnIDs.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRException.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

class NodeIDSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        ASSERT_NE(nodeIDs, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _nodes.push_back((*nodeIDs)[rowIndex].getValue());
        }
    }

    void sortedNodes(std::vector<uint64_t>& nodes) const {
        nodes = _nodes;
        std::sort(nodes.begin(), nodes.end());
    }

private:
    std::vector<uint64_t> _nodes;
};

// MATCH (n) WHERE n.age = 32 RETURN n
constexpr const char* NULLABLE_COMPARISON_FILTER_PROGRAM = R"mlir(
func.func @main() {
  %age = nl.get_property_type("age")
  %k = nl.constant(32 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %v = nl.get_node_properties(%a, %age) : !nl.chunk<!storage.nullable<i64>>
    %mask = nl.eq %v, %k : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    %a2 = nl.filter %mask, (%a) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%a2) : !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

// The same comparison declared two-valued: its kernel still writes a nullable mask
constexpr const char* NULLABLE_COMPARISON_DECLARED_TWO_VALUED_PROGRAM = R"mlir(
func.func @main() {
  %age = nl.get_property_type("age")
  %k = nl.constant(32 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %v = nl.get_node_properties(%a, %age) : !nl.chunk<!storage.nullable<i64>>
    %mask = nl.eq %v, %k : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<i1>
    %a2 = nl.filter %mask, (%a) : (!nl.chunk<i1>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%a2) : !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

// MATCH (n) WHERE n = 1 RETURN n: an ID column carries an OPTIONAL MATCH's null in the ID,
// so the comparison is three-valued
constexpr const char* ENTITY_COMPARISON_FILTER_PROGRAM = R"mlir(
func.func @main() {
  %k = nl.constant(1 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %mask = nl.eq %a, %k : (!nl.chunk<!storage.node_id>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    %a2 = nl.filter %mask, (%a) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%a2) : !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

// The same comparison declared two-valued: its kernel still writes a nullable mask
constexpr const char* ENTITY_COMPARISON_DECLARED_TWO_VALUED_PROGRAM = R"mlir(
func.func @main() {
  %k = nl.constant(1 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %mask = nl.eq %a, %k : (!nl.chunk<!storage.node_id>, !nl.chunk<i64>) -> !nl.chunk<i1>
    %a2 = nl.filter %mask, (%a) : (!nl.chunk<i1>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%a2) : !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

// MATCH (n) WHERE 1 = 2 RETURN n, with the comparison declared three-valued: its kernel
// writes a plain mask
constexpr const char* TWO_VALUED_COMPARISON_DECLARED_NULLABLE_PROGRAM = R"mlir(
func.func @main() {
  %k1 = nl.constant(1 : i64)
  %k2 = nl.constant(2 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %mask = nl.eq %k1, %k2 : (!nl.chunk<i64>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    %a2 = nl.filter %mask, (%a) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%a2) : !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

// MATCH (n) WHERE NOT n.age = 32 RETURN n, with the NOT declared two-valued
constexpr const char* NOT_OF_NULLABLE_PREDICATE_DECLARED_TWO_VALUED_PROGRAM = R"mlir(
func.func @main() {
  %age = nl.get_property_type("age")
  %k = nl.constant(32 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %v = nl.get_node_properties(%a, %age) : !nl.chunk<!storage.nullable<i64>>
    %eq = nl.eq %v, %k : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    %mask = nl.not %eq : (!nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<i1>
    %a2 = nl.filter %mask, (%a) : (!nl.chunk<i1>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%a2) : !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

// MATCH (n) WHERE n.age + 1 = 33 RETURN n, with the sum declared never null
constexpr const char* NULLABLE_SUM_DECLARED_PLAIN_PROGRAM = R"mlir(
func.func @main() {
  %age = nl.get_property_type("age")
  %one = nl.constant(1 : i64)
  %k = nl.constant(33 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %v = nl.get_node_properties(%a, %age) : !nl.chunk<!storage.nullable<i64>>
    %sum = nl.add %v, %one : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<i64>
    %mask = nl.eq %sum, %k : (!nl.chunk<i64>, !nl.chunk<i64>) -> !nl.chunk<i1>
    %a2 = nl.filter %mask, (%a) : (!nl.chunk<i1>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%a2) : !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

class KernelResultNullabilityTest : public TuringTest {
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
        NodeIDSink sink;
        try {
            runProgram(programText, sink);
            ADD_FAILURE() << "The program ran instead of being rejected";
        } catch (const IRException& exception) {
            const std::string_view message = exception.what();
            EXPECT_NE(message.find(expectedMessage), std::string_view::npos) << message;
        }

        std::vector<uint64_t> nodes;
        sink.sortedNodes(nodes);
        EXPECT_TRUE(nodes.empty());
    }

    std::unique_ptr<Graph> _graph;
};

}

TEST_F(KernelResultNullabilityTest, nullableComparisonFilter) {
    NodeIDSink sink;
    runProgram(NULLABLE_COMPARISON_FILTER_PROGRAM, sink);

    std::vector<uint64_t> nodes;
    sink.sortedNodes(nodes);

    const std::vector<uint64_t> remyAndAdam {0, 1};
    EXPECT_EQ(nodes, remyAndAdam);
}

TEST_F(KernelResultNullabilityTest, entityComparisonFilter) {
    NodeIDSink sink;
    runProgram(ENTITY_COMPARISON_FILTER_PROGRAM, sink);

    std::vector<uint64_t> nodes;
    sink.sortedNodes(nodes);

    const std::vector<uint64_t> adam {1};
    EXPECT_EQ(nodes, adam);
}

TEST_F(KernelResultNullabilityTest, nullableComparisonDeclaredTwoValuedIsRejected) {
    expectRejected(NULLABLE_COMPARISON_DECLARED_TWO_VALUED_PROGRAM, "nl.eq declares a !nl.chunk<i1> result");
}

TEST_F(KernelResultNullabilityTest, entityComparisonDeclaredTwoValuedIsRejected) {
    expectRejected(ENTITY_COMPARISON_DECLARED_TWO_VALUED_PROGRAM, "nl.eq declares a !nl.chunk<i1> result");
}

TEST_F(KernelResultNullabilityTest, twoValuedComparisonDeclaredNullableIsRejected) {
    expectRejected(TWO_VALUED_COMPARISON_DECLARED_NULLABLE_PROGRAM, "nl.eq declares a !nl.chunk<!storage.nullable<i1>> result");
}

TEST_F(KernelResultNullabilityTest, notOfNullablePredicateDeclaredTwoValuedIsRejected) {
    expectRejected(NOT_OF_NULLABLE_PREDICATE_DECLARED_TWO_VALUED_PROGRAM, "nl.not declares a !nl.chunk<i1> result");
}

TEST_F(KernelResultNullabilityTest, nullableSumDeclaredPlainIsRejected) {
    expectRejected(NULLABLE_SUM_DECLARED_PLAIN_PROGRAM, "nl.add declares a !nl.chunk<i64> result");
}
