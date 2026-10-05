#include <gtest/gtest.h>

#include <algorithm>
#include <span>
#include <string>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "llvm/Support/raw_ostream.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "LocalMemory.h"
#include "SimpleGraph.h"
#include "TuringTest.h"

#include "IRTestOps.h"

using namespace db;
using namespace turing::test;

namespace {

using LabelAlternatives = std::vector<std::vector<std::string>>;

class CollectingNodeSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const ColumnNodeIDs* nodes = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        ASSERT_NE(nodes, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _nodes.push_back((*nodes)[rowIndex].getValue());
        }
    }

    void sortedNodes(std::vector<uint64_t>& nodes) const {
        nodes = _nodes;
        std::sort(nodes.begin(), nodes.end());
    }

private:
    std::vector<uint64_t> _nodes;
};

// MATCH (n) WHERE n:Person OR n:Interest RETURN n
const char* const orOfTwoLabels = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ls2 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %i = db.check_label_constraint(%ls2, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ok = db.or %p, %i : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%ok, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n) WHERE n:Person:Founder OR n:Interest:SoftwareEngineering OR n:Exotic RETURN n
const char* const orOfThreeConjunctions = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %f = db.check_label_constraint(%ls1, ["Person", "Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ls2 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %s = db.check_label_constraint(%ls2, ["Interest", "SoftwareEngineering"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %fs = db.or %f, %s : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %ls3 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %e = db.check_label_constraint(%ls3, ["Exotic"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ok = db.or %fs, %e : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%ok, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n) WHERE n:Person OR n:Person:Founder RETURN n: every Founder here is a Person
const char* const orOfASubsumedConjunction = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ls2 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %f = db.check_label_constraint(%ls2, ["Person", "Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ok = db.or %f, %p : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%ok, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n) WHERE n:Person:Founder OR n:Founder:Person RETURN n
const char* const orOfAReorderedConjunction = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %pf = db.check_label_constraint(%ls1, ["Person", "Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ls2 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %fp = db.check_label_constraint(%ls2, ["Founder", "Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ok = db.or %pf, %fp : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%ok, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n) WHERE n:Person AND n:Founder RETURN n
const char* const andOfTwoLabels = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ls2 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %f = db.check_label_constraint(%ls2, ["Founder", "Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ok = db.and %p, %f : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%ok, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n) WHERE (n:Person AND n:Bioinformatics) OR n:Sales RETURN n
const char* const andInsideAnOr = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ls2 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %b = db.check_label_constraint(%ls2, ["Bioinformatics"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %pb = db.and %p, %b : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %ls3 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %s = db.check_label_constraint(%ls3, ["Sales"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ok = db.or %pb, %s : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%ok, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n) WHERE (n:Person OR n:Interest) AND n:Exotic RETURN n
const char* const andOfAnOrAndALabel = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ls2 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %i = db.check_label_constraint(%ls2, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %pi = db.or %p, %i : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %ls3 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %e = db.check_label_constraint(%ls3, ["Exotic"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ok = db.and %pi, %e : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%ok, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n) WHERE (n:Person OR n:Interest) AND (n:Founder OR n:Exotic) RETURN n
const char* const andOfTwoOrs = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ls2 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %i = db.check_label_constraint(%ls2, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %pi = db.or %p, %i : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %ls3 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %f = db.check_label_constraint(%ls3, ["Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ls4 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %e = db.check_label_constraint(%ls4, ["Exotic"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %fe = db.or %f, %e : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %ok = db.and %pi, %fe : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%ok, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)-->(m) WHERE n:Person AND n:Founder RETURN n, m: one filter per conjunct
const char* const stackedLabelFilters = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %s1, %t1 = db.filter(%p, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %ls2 = db.get_node_label_set(%s1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %f = db.check_label_constraint(%ls2, ["Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %s2, %t2 = db.filter(%f, {%s1, %t1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%s2, %t2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The first filter's rows are also output, so they must keep the nodes Founder turns away.
const char* const stackedLabelFiltersReadBetween = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %s1, %t1 = db.filter(%p, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %ls2 = db.get_node_label_set(%s1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %f = db.check_label_constraint(%ls2, ["Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %s2, %t2 = db.filter(%f, {%s1, %t1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%s2, %t1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The two filters test the two ends of the edge.
const char* const stackedLabelFiltersOverDifferentNodes = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %s1, %t1 = db.filter(%p, {%s, %t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %ls2 = db.get_node_label_set(%t1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %i = db.check_label_constraint(%ls2, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %s2, %t2 = db.filter(%i, {%s1, %t1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%s2, %t2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n:Person)-->(m) WHERE n:Founder AND m:Interest RETURN n: the hop sits between the
// filter on n and the one on its source column
const char* const labelFilterAfterAHop = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %nf = db.filter(%p, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%nf, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %ls2 = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %f = db.check_label_constraint(%ls2, ["Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %t1, %e1, %s1, %et1 = db.filter(%f, {%t, %e, %s, %et}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>)
  %ls3 = db.get_node_label_set(%t1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %i = db.check_label_constraint(%ls3, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %t2, %e2, %s2, %et2 = db.filter(%i, {%t1, %e1, %s1, %et1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>)
  db.output(%s2) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n:Person)-->(m)-->(k) WHERE n:Founder RETURN n: the second hop carries n
const char* const labelFilterOnACarriedColumn = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %nf = db.filter(%p, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %s, %e, %et, %m = db.get_out_edges(%nf, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %s2, %e2, %et2, %k, %n2 = db.get_out_edges(%m, {%s}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %ls2 = db.get_node_label_set(%n2) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %f = db.check_label_constraint(%ls2, ["Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %n3, %k3 = db.filter(%f, {%n2, %k}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%n3) : !db.column<!storage.node_id>
  return
}
)mlir";

// The hop's source column is also output unfiltered, so the hop must keep every row.
const char* const labelFilterAfterAHopReadElsewhere = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %ls = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %f = db.check_label_constraint(%ls, ["Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %sf = db.filter(%f, {%s}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%sf, %t) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The check is on the node the hop reaches, which no filter above the hop can see.
const char* const labelFilterOnTheReachedEnd = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %t = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %ls = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %i = db.check_label_constraint(%ls, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %tf = db.filter(%i, {%t}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%tf) : !db.column<!storage.node_id>
  return
}
)mlir";

// A label no node carries makes its alternative match nothing, not the whole check.
const char* const orWithAnUnknownLabel = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ls2 = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %u = db.check_label_constraint(%ls2, ["NoSuchLabel"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ok = db.or %p, %u : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %nf = db.filter(%ok, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

// The checks are over the two ends of an edge, so they are about different nodes.
const char* const orOverDifferentNodes = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %ls1 = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ls2 = db.get_node_label_set(%t) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %i = db.check_label_constraint(%ls2, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ok = db.or %p, %i : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %sf = db.filter(%ok, {%s}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%sf) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const orWithANonLabelCheck = R"mlir(
func.func @main() {
  %s, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %k = db.check_edge_type_constraint(%et, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %ls = db.get_node_label_set(%s) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %p = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %ok = db.or %k, %p : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %sf = db.filter(%ok, {%s}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%sf) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const nestedAlternatives = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok = db.check_label_constraint(%ls, [["Person", "Founder"], ["Interest"]]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %nf = db.filter(%ok, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const singleNestedAlternative = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok = db.check_label_constraint(%ls, [["Person", "Founder"]]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %nf = db.filter(%ok, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const emptyAlternative = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %ls = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok = db.check_label_constraint(%ls, [["Person"], []]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %nf = db.filter(%ok, {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%nf) : !db.column<!storage.node_id>
  return
}
)mlir";

}

class FuseLabelPredicatesTest : public TuringTest {
protected:
    void initialize() override {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runFuse(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFuseLabelPredicates());

        return mlir::succeeded(passManager.run(module));
    }

    void print(mlir::ModuleOp module, std::string& text) {
        llvm::raw_string_ostream stream(text);
        module.print(stream);
    }

    void expectAlternatives(mlir::db::CheckLabelConstraint check, const LabelAlternatives& expected) {
        const mlir::ArrayAttr alternatives = check.getAlternatives();
        ASSERT_EQ(alternatives.size(), expected.size());

        for (size_t index = 0; index < expected.size(); index++) {
            const mlir::ArrayAttr labels = mlir::cast<mlir::ArrayAttr>(alternatives[index]);
            ASSERT_EQ(labels.size(), expected[index].size());

            for (size_t labelIndex = 0; labelIndex < labels.size(); labelIndex++) {
                EXPECT_EQ(mlir::cast<mlir::StringAttr>(labels[labelIndex]).getValue(), expected[index][labelIndex]);
            }
        }
    }

    // One check and one label-set read are left, and the ORs that joined them are gone.
    void expectFusedTo(const char* programText, const LabelAlternatives& expected) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        ASSERT_TRUE(module);
        ASSERT_TRUE(runFuse(*module));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

        llvm::SmallVector<mlir::db::CheckLabelConstraint> checks = collect<mlir::db::CheckLabelConstraint>(module.get());
        ASSERT_EQ(checks.size(), 1u);
        expectAlternatives(checks.front(), expected);

        EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 1u);
        EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 0u);
        EXPECT_EQ(countOps<mlir::db::AndOp>(*module), 0u);
    }

    void expectUntouched(const char* programText) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        ASSERT_TRUE(module);
        ASSERT_TRUE(runFuse(*module));

        EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 1u);
    }

    void runNodes(mlir::ModuleOp module, const GraphView& view, std::vector<uint64_t>& nodes) {
        const mlir::func::FuncOp dbFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
        DBLowering lowering(&_context, &view);
        lowering.lower(dbFunction, *nlModule);

        CollectingNodeSink sink;
        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();

        sink.sortedNodes(nodes);
    }

    void expectSameRowsAfterPass(const char* programText) {
        auto graph = Graph::create();
        SimpleGraph::createSimpleGraph(graph.get());

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        const mlir::OwningOpRef<mlir::ModuleOp> before = parse(programText);
        ASSERT_TRUE(before);
        std::vector<uint64_t> beforeRows;
        runNodes(*before, view, beforeRows);

        const mlir::OwningOpRef<mlir::ModuleOp> after = parse(programText);
        ASSERT_TRUE(after);
        ASSERT_TRUE(runFuse(*after));
        std::vector<uint64_t> afterRows;
        runNodes(*after, view, afterRows);

        EXPECT_FALSE(beforeRows.empty());
        EXPECT_EQ(afterRows, beforeRows);
    }

    mlir::MLIRContext _context;
};

TEST_F(FuseLabelPredicatesTest, foldsAnOrIntoTwoAlternatives) {
    expectFusedTo(orOfTwoLabels, {{"Person"}, {"Interest"}});
}

TEST_F(FuseLabelPredicatesTest, foldsAChainOfConjunctions) {
    expectFusedTo(orOfThreeConjunctions, {{"Person", "Founder"}, {"Interest", "SoftwareEngineering"}, {"Exotic"}});
}

TEST_F(FuseLabelPredicatesTest, dropsAnAlternativeAnotherSubsumes) {
    expectFusedTo(orOfASubsumedConjunction, {{"Person"}});
}

TEST_F(FuseLabelPredicatesTest, dropsAReorderedAlternative) {
    expectFusedTo(orOfAReorderedConjunction, {{"Person", "Founder"}});
}

TEST_F(FuseLabelPredicatesTest, foldsAnAndIntoOneConjunction) {
    expectFusedTo(andOfTwoLabels, {{"Person", "Founder"}});
}

TEST_F(FuseLabelPredicatesTest, foldsAnAndInsideAnOr) {
    expectFusedTo(andInsideAnOr, {{"Person", "Bioinformatics"}, {"Sales"}});
}

TEST_F(FuseLabelPredicatesTest, distributesAnAndOverAnOr) {
    expectFusedTo(andOfAnOrAndALabel, {{"Person", "Exotic"}, {"Interest", "Exotic"}});
}

// Each OR folds, but the AND of the two would need an alternative per pair.
TEST_F(FuseLabelPredicatesTest, leavesAnAndOfTwoOrsAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(andOfTwoOrs);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 2u);
    EXPECT_EQ(countOps<mlir::db::OrOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::AndOp>(*module), 1u);
}

TEST_F(FuseLabelPredicatesTest, mergesStackedLabelFilters) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(stackedLabelFilters);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::CheckLabelConstraint> checks = collect<mlir::db::CheckLabelConstraint>(module.get());
    ASSERT_EQ(checks.size(), 1u);
    expectAlternatives(checks.front(), {{"Person", "Founder"}});

    EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(FuseLabelPredicatesTest, leavesStackedFiltersReadBetweenAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(stackedLabelFiltersReadBetween);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 2u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 2u);
}

TEST_F(FuseLabelPredicatesTest, leavesStackedFiltersOverDifferentNodesAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(stackedLabelFiltersOverDifferentNodes);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 2u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 2u);
}

// The Founder filter moves above the hop and merges with the Person one, while the Interest
// filter on the node the hop reaches stays below it.
TEST_F(FuseLabelPredicatesTest, mergesALabelFilterAcrossAHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(labelFilterAfterAHop);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(module.get());
    ASSERT_EQ(hops.size(), 1u);

    mlir::db::FilterOp aboveHop = hops.front().getInputNodes().getDefiningOp<mlir::db::FilterOp>();
    ASSERT_TRUE(aboveHop);
    mlir::db::CheckLabelConstraint check = aboveHop.getMask().getDefiningOp<mlir::db::CheckLabelConstraint>();
    ASSERT_TRUE(check);
    expectAlternatives(check, {{"Person", "Founder"}});

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 2u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 2u);
}

TEST_F(FuseLabelPredicatesTest, mergesALabelFilterAcrossTwoHops) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(labelFilterOnACarriedColumn);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

    llvm::SmallVector<mlir::db::CheckLabelConstraint> checks = collect<mlir::db::CheckLabelConstraint>(module.get());
    ASSERT_EQ(checks.size(), 1u);
    expectAlternatives(checks.front(), {{"Person", "Founder"}});

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(FuseLabelPredicatesTest, leavesAFilterAfterAHopReadElsewhereAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(labelFilterAfterAHopReadElsewhere);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(module.get());
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_FALSE(hops.front().getInputNodes().getDefiningOp<mlir::db::FilterOp>());
}

TEST_F(FuseLabelPredicatesTest, leavesAFilterOnTheReachedEndAlone) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(labelFilterOnTheReachedEnd);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runFuse(*module));

    llvm::SmallVector<mlir::db::GetOutEdges> hops = collect<mlir::db::GetOutEdges>(module.get());
    ASSERT_EQ(hops.size(), 1u);
    EXPECT_FALSE(hops.front().getInputNodes().getDefiningOp<mlir::db::FilterOp>());
}

TEST_F(FuseLabelPredicatesTest, leavesChecksOverDifferentNodesAlone) {
    expectUntouched(orOverDifferentNodes);
}

TEST_F(FuseLabelPredicatesTest, leavesAnOrWithANonLabelCheckAlone) {
    expectUntouched(orWithANonLabelCheck);
}

TEST_F(FuseLabelPredicatesTest, orEmitsTheSameRows) {
    expectSameRowsAfterPass(orOfTwoLabels);
}

TEST_F(FuseLabelPredicatesTest, orOfConjunctionsEmitsTheSameRows) {
    expectSameRowsAfterPass(orOfThreeConjunctions);
}

TEST_F(FuseLabelPredicatesTest, subsumedAlternativeEmitsTheSameRows) {
    expectSameRowsAfterPass(orOfASubsumedConjunction);
}

TEST_F(FuseLabelPredicatesTest, andEmitsTheSameRows) {
    expectSameRowsAfterPass(andOfTwoLabels);
}

TEST_F(FuseLabelPredicatesTest, andInsideAnOrEmitsTheSameRows) {
    expectSameRowsAfterPass(andInsideAnOr);
}

TEST_F(FuseLabelPredicatesTest, andOverAnOrEmitsTheSameRows) {
    expectSameRowsAfterPass(andOfAnOrAndALabel);
}

TEST_F(FuseLabelPredicatesTest, andOfTwoOrsEmitsTheSameRows) {
    expectSameRowsAfterPass(andOfTwoOrs);
}

TEST_F(FuseLabelPredicatesTest, filterAcrossAHopEmitsTheSameRows) {
    expectSameRowsAfterPass(labelFilterAfterAHop);
}

TEST_F(FuseLabelPredicatesTest, filterAcrossTwoHopsEmitsTheSameRows) {
    expectSameRowsAfterPass(labelFilterOnACarriedColumn);
}

TEST_F(FuseLabelPredicatesTest, unknownLabelEmitsTheSameRows) {
    expectSameRowsAfterPass(orWithAnUnknownLabel);
}

TEST_F(FuseLabelPredicatesTest, printsSeveralAlternativesNested) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(nestedAlternatives);
    ASSERT_TRUE(module);

    std::string text;
    print(*module, text);
    EXPECT_NE(text.find(R"([["Person", "Founder"], ["Interest"]])"), std::string::npos);
}

TEST_F(FuseLabelPredicatesTest, printsOneAlternativeFlat) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(singleNestedAlternative);
    ASSERT_TRUE(module);

    std::string text;
    print(*module, text);
    EXPECT_NE(text.find(R"((%1, ["Person", "Founder"]))"), std::string::npos);
}

TEST_F(FuseLabelPredicatesTest, rejectsAnEmptyAlternative) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(emptyAlternative);
    EXPECT_FALSE(module);
}
