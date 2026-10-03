#include <gtest/gtest.h>

#include <memory>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "TuringException.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "StorageDialect.h"

#include "IRTestRows.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// The body reads %arg0 - the chunk of the enclosing loop - where it should read %arg3,
// the carried column the op repeats over the elements of each row. The yielded value
// then holds the 8 rows of the scan while the row tags beside it hold the 16 element
// rows of two elements per node, which is the shape a comprehension carrying nothing
// produces for a body that names an outer column.
const char* const misalignedYieldProgram = R"mlir(
func.func @main() {
  %0 = nl.get_property_type("age")
  %1 = nl.constant([1, 2])
  %2 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %3 = nl.broadcast_constant %1, %arg0 : (!nl.chunk<!storage.list<i64>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.list<i64>>
    %4 = nl.list_comprehension(%3, {%arg0}) {
    ^bb0(%arg1: !nl.chunk<!storage.nullable<i64>>, %arg2: !nl.chunk<ui64>, %arg3: !nl.chunk<!storage.node_id>):
      %5 = nl.get_node_properties(%arg0, %0) : !nl.chunk<!storage.nullable<i64>>
      nl.comprehension_yield %arg2, %5 : !nl.chunk<!storage.nullable<i64>>
    } : (!nl.chunk<!storage.list<i64>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.nullable<!storage.list<i64>>>
    nl.output(%4) names ["c"] : !nl.chunk<!storage.nullable<!storage.list<i64>>>
  }
  return
}
)mlir";

}

// A comprehension whose yielded value is not row-aligned with its row tags. The executor
// indexes the value column by the element count and the staged counts by the tag value,
// so a program the verifier lets through must be rejected rather than read past the end
// of either - the guard runMakeList carries for the same invariant.
class ListComprehensionRowAlignmentTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    void run(const char* programText) {
        const mlir::ParserConfig parserConfig(&_context);
        mlir::OwningOpRef<mlir::ModuleOp> module =
            mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        NullSink sink;
        LocalMemory memory;
        NLInterpreter interpreter(*module, &view, &sink, &memory);
        interpreter.run();
    }

    mlir::MLIRContext _context;
    std::unique_ptr<Graph> _graph;
};

TEST_F(ListComprehensionRowAlignmentTest, rejectsAYieldThatIsNotRowAligned) {
    EXPECT_THROW(run(misalignedYieldProgram), TuringException);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
