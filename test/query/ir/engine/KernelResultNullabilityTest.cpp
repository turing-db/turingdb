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
constexpr const char* nullableComparisonFilterProgram = R"mlir(
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
constexpr const char* nullableComparisonDeclaredTwoValuedProgram = R"mlir(
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

// MATCH (n) WHERE n = 1 RETURN n
constexpr const char* twoValuedComparisonFilterProgram = R"mlir(
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

// The same comparison declared three-valued: its kernel still writes a plain mask
constexpr const char* twoValuedComparisonDeclaredNullableProgram = R"mlir(
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

// MATCH (n) WHERE NOT n.age = 32 RETURN n, with the NOT declared two-valued
constexpr const char* notOfNullablePredicateDeclaredTwoValuedProgram = R"mlir(
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
constexpr const char* nullableSumDeclaredPlainProgram = R"mlir(
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
    runProgram(nullableComparisonFilterProgram, sink);

    std::vector<uint64_t> nodes;
    sink.sortedNodes(nodes);

    const std::vector<uint64_t> remyAndAdam {0, 1};
    EXPECT_EQ(nodes, remyAndAdam);
}

TEST_F(KernelResultNullabilityTest, twoValuedComparisonFilter) {
    NodeIDSink sink;
    runProgram(twoValuedComparisonFilterProgram, sink);

    std::vector<uint64_t> nodes;
    sink.sortedNodes(nodes);

    const std::vector<uint64_t> adam {1};
    EXPECT_EQ(nodes, adam);
}

TEST_F(KernelResultNullabilityTest, nullableComparisonDeclaredTwoValuedIsRejected) {
    expectRejected(nullableComparisonDeclaredTwoValuedProgram, "nl.eq declares a !nl.chunk<i1> result");
}

TEST_F(KernelResultNullabilityTest, twoValuedComparisonDeclaredNullableIsRejected) {
    expectRejected(twoValuedComparisonDeclaredNullableProgram, "nl.eq declares a !nl.chunk<!storage.nullable<i1>> result");
}

TEST_F(KernelResultNullabilityTest, notOfNullablePredicateDeclaredTwoValuedIsRejected) {
    expectRejected(notOfNullablePredicateDeclaredTwoValuedProgram, "nl.not declares a !nl.chunk<i1> result");
}

TEST_F(KernelResultNullabilityTest, nullableSumDeclaredPlainIsRejected) {
    expectRejected(nullableSumDeclaredPlainProgram, "nl.add declares a !nl.chunk<i64> result");
}
