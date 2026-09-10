#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <span>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "columns/Column.h"
#include "iterators/ChunkConfig.h"
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

class RowCountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }

private:
    size_t _rowCount {0};
};

// The build columns are not operands of nl.hash_join_probe - they live in the handle - so
// their chunk types are spelled in its iterator, and nothing in NLOps.td ties those to the
// columns the collect appended. Here the last chunk claims a node ID column where the
// collect appended a nullable string, so the buffer holding string views would be gathered
// as a column of IDs.
constexpr const char* const mistypedBuildResultProgram = R"mlir(
func.func @main() {
  %state = nl.hash_join_buffer build_key 1 probe_key 1
  %nameType = nl.get_property_type("name")
  %build = nl.scan_nodes()
  nl.for %b in %build : !nl.iter<!nl.chunk<!storage.node_id>> {
    %bname = nl.get_node_properties(%b, %nameType) : !nl.chunk<!storage.nullable<!storage.string>>
    nl.hash_join_collect %state, (%b, %bname) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>
  }
  %probe = nl.scan_nodes()
  nl.for %p in %probe : !nl.iter<!nl.chunk<!storage.node_id>> {
    %pname = nl.get_node_properties(%p, %nameType) : !nl.chunk<!storage.nullable<!storage.string>>
    %joined = nl.hash_join_probe %state, (%p, %pname) : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>>
    nl.for %n, %name, %m, %mname in %joined : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
      nl.output(%n, %m) names ["n", "m"] : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
    }
  }
  return
}
)mlir";

// The same program with the build results spelled as the collect appended them, which is
// what the lowering emits and what the translator has to accept.
constexpr const char* const buildResultProgram = R"mlir(
func.func @main() {
  %state = nl.hash_join_buffer build_key 1 probe_key 1
  %nameType = nl.get_property_type("name")
  %build = nl.scan_nodes()
  nl.for %b in %build : !nl.iter<!nl.chunk<!storage.node_id>> {
    %bname = nl.get_node_properties(%b, %nameType) : !nl.chunk<!storage.nullable<!storage.string>>
    nl.hash_join_collect %state, (%b, %bname) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>
  }
  %probe = nl.scan_nodes()
  nl.for %p in %probe : !nl.iter<!nl.chunk<!storage.node_id>> {
    %pname = nl.get_node_properties(%p, %nameType) : !nl.chunk<!storage.nullable<!storage.string>>
    %joined = nl.hash_join_probe %state, (%p, %pname) : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>>
    nl.for %n, %name, %m, %mname in %joined : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>> {
      nl.output(%n, %m) names ["n", "m"] : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
    }
  }
  return
}
)mlir";

}

// What an nl.hash_join_probe declares its build chunks as, which is what the buffers the
// collect allocated are read back through.
class HashJoinProbeResultsTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void runProgram(const char* programText, NLOutputSink& sink) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        LocalMemory memory;
        NLInterpreter interpreter(*module, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();
    }

    std::unique_ptr<Graph> _graph;
};

TEST_F(HashJoinProbeResultsTest, rejectsABuildResultTypedOtherThanItsBuffer) {
    RowCountingSink sink;

    EXPECT_THROW(runProgram(mistypedBuildResultProgram, sink), IRException);
}

// Every node carries its own name, so the join is the diagonal.
TEST_F(HashJoinProbeResultsTest, joinsOnABuildResultTypedAsItsBuffer) {
    RowCountingSink sink;
    runProgram(buildResultProgram, sink);

    EXPECT_EQ(sink.getRowCount(), 18u);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
