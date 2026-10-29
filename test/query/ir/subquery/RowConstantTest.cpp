#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "IRTestRows.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "StorageDialect.h"

#include "Graph.h"
#include "LocalMemory.h"
#include "SimpleGraph.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// CALL (p) { RETURN p.name AS name }: the property of the constant is read once per run
const char* const propertyProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %r:2 = db.call_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    %name = db.get_node_properties(%pc, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    db.subquery_yield {%name} : {!db.column<!storage.nullable<!storage.string>>}
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>)
  db.output(%r#1) : !db.column<!storage.nullable<!storage.string>>
  return
}
)mlir";

// CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN p.name AS name, count(i) AS c }: the
// hop starts from the constant, and the name rides beside the one group
const char* const countProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %r:3 = db.call_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    %s, %e, %et, %i = db.get_out_edges_by_type(%pc, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %name = db.get_node_properties(%pc, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    %key = db.constant(true)
    %g:2 = db.group_aggregate(%key, %i) keys 1 aggregates [count] : (!db.column<i1>, !db.column<!storage.node_id>) -> (!db.column<i1>, !db.column<none>)
    db.subquery_yield {%name, %g#1} : {!db.column<!storage.nullable<!storage.string>>, !db.column<none>}
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>, !db.column<none>)
  db.output(%r#1, %r#2) : !db.column<!storage.nullable<!storage.string>>, !db.column<none>
  return
}
)mlir";

// The same over KNOWS_WELL, which only Remy and Adam have: the others make no group
const char* const emptyGroupProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %r:3 = db.call_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    %s, %e, %et, %i = db.get_out_edges_by_type(%pc, ["KNOWS_WELL"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %name = db.get_node_properties(%pc, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    %key = db.constant(true)
    %g:2 = db.group_aggregate(%key, %i) keys 1 aggregates [count] : (!db.column<i1>, !db.column<!storage.node_id>) -> (!db.column<i1>, !db.column<none>)
    db.subquery_yield {%name, %g#1} : {!db.column<!storage.nullable<!storage.string>>, !db.column<none>}
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>, !db.column<none>)
  db.output(%r#1, %r#2) : !db.column<!storage.nullable<!storage.string>>, !db.column<none>
  return
}
)mlir";

// CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN p.name AS name, i.name AS interest }:
// the constant name is laid out over the rows of the hop
const char* const valueBesideRowsProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %r:3 = db.call_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    %s, %e, %et, %i = db.get_out_edges_by_type(%pc, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %name = db.get_node_properties(%pc, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    %interest = db.get_node_properties(%i, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    db.subquery_yield {%name, %interest} : {!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>}
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>)
  db.output(%r#1, %r#2) : !db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>
  return
}
)mlir";

// CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN p AS q, i.name AS interest }: the
// constant node itself is laid out over the rows, and read again past the CALL
const char* const nodeBesideRowsProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %r:3 = db.call_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    %s, %e, %et, %i = db.get_out_edges_by_type(%pc, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %interest = db.get_node_properties(%i, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    db.subquery_yield {%pc, %interest} : {!db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>}
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>)
  %name = db.get_node_properties(%r#1, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
  db.output(%name, %r#2) : !db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>
  return
}
)mlir";

// CALL (p) { MATCH (x:Person) MATCH (p)-[:INTERESTED_IN]->(i) RETURN p.name AS name,
// count(i) AS c }: the hop from the constant carries the rows of x, so it starts from the
// constant laid out over them
const char* const hopOverRowsProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %r:3 = db.call_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    %x = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    %s, %e, %et, %i, %xh = db.get_out_edges_by_type(%pc, ["INTERESTED_IN"], {%x}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
    %name = db.get_node_properties(%pc, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    %key = db.constant(true)
    %g:2 = db.group_aggregate(%key, %i) keys 1 aggregates [count] : (!db.column<i1>, !db.column<!storage.node_id>) -> (!db.column<i1>, !db.column<none>)
    db.subquery_yield {%name, %g#1} : {!db.column<!storage.nullable<!storage.string>>, !db.column<none>}
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>, !db.column<none>)
  db.output(%r#1, %r#2) : !db.column<!storage.nullable<!storage.string>>, !db.column<none>
  return
}
)mlir";

// CALL (p) { MATCH (x:Person) WHERE x = p RETURN x.name AS same }: the comparison reads
// the constant beside the rows of x
const char* const equalityProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %r:2 = db.call_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    %x = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    %same = db.eq %x, %pc : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
    %kept = db.filter(%same, {%x}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    %name = db.get_node_properties(%kept, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    db.subquery_yield {%name} : {!db.column<!storage.nullable<!storage.string>>}
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>)
  %name = db.get_node_properties(%r#0, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
  db.output(%name, %r#1) : !db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>
  return
}
)mlir";

// CALL (p) { MATCH (x:Person) RETURN count(p) AS c }: the count folds the constant over the
// rows of x
const char* const countEntityProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %r:2 = db.call_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    %x = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    %key = db.constant(true)
    %g:2 = db.group_aggregate(%key, %pc) keys 1 aggregates [count] : (!db.column<i1>, !db.column<!storage.node_id>) -> (!db.column<i1>, !db.column<none>)
    db.subquery_yield {%g#1} : {!db.column<none>}
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%r#1) : !db.column<none>
  return
}
)mlir";

// CALL (p) { MATCH (x:Person) RETURN size(collect(p)) AS n }: the collect does the same
const char* const collectEntityProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %r:2 = db.call_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    %x = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    %key = db.constant(true)
    %c:2 = db.collect(%key, %pc) keys 1 : (!db.column<i1>, !db.column<!storage.node_id>) -> (!db.column<i1>, !db.column<!storage.list<!storage.node_id>>)
    %size = db.size(%c#1) : (!db.column<!storage.list<!storage.node_id>>) -> !db.column<none>
    db.subquery_yield {%size} : {!db.column<none>}
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%r#1) : !db.column<none>
  return
}
)mlir";

// MATCH (a)-[r:KNOWS_WELL]->(b) CALL (r) { RETURN type(r) AS t }, and the edge itself
// laid out over the rows of a MATCH (x:Person) in the body
const char* const edgeProgram = R"mlir(
func.func @main() {
  %a, %r, %rt, %b = db.scan_edges_by_type(["KNOWS_WELL"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %c:4 = db.call_subquery(%r) {
  ^bb0(%rIn: !db.column<!storage.edge_id>):
    %rc = db.row_constant %rIn : !db.column<!storage.edge_id>
    %type = db.edge_type(%rc) : (!db.column<!storage.edge_id>) -> !db.column<none>
    %x = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    %xname = db.get_node_properties(%x, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    db.subquery_yield {%type, %rc, %xname} : {!db.column<none>, !db.column<!storage.edge_id>, !db.column<!storage.nullable<!storage.string>>}
  } : (!db.column<!storage.edge_id>) -> (!db.column<!storage.edge_id>, !db.column<none>, !db.column<!storage.edge_id>, !db.column<!storage.nullable<!storage.string>>)
  %source = db.start_node(%c#2) : (!db.column<!storage.edge_id>) -> !db.column<none>
  db.output(%c#1, %source, %c#3) : !db.column<none>, !db.column<none>, !db.column<!storage.nullable<!storage.string>>
  return
}
)mlir";

// MATCH p = (a)-[:KNOWS_WELL]->(b) CALL (p) { MATCH (x:Person) RETURN length(p) AS l, p AS q,
// x.name AS name }: the length is read once per run, the path laid out over the rows of x
const char* const pathProgram = R"mlir(
func.func @main() {
  %a, %r, %rt, %b = db.scan_edges_by_type(["KNOWS_WELL"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  %path = db.make_path(%a, %r, %b) : (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>) -> !db.column<!storage.path>
  %c:4 = db.call_subquery(%path) {
  ^bb0(%pathIn: !db.column<!storage.path>):
    %pc = db.row_constant %pathIn : !db.column<!storage.path>
    %length = db.path_elements(%pc) kind length : (!db.column<!storage.path>) -> !db.column<!storage.nullable<ui64>>
    %x = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    %xname = db.get_node_properties(%x, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    db.subquery_yield {%length, %pc, %xname} : {!db.column<!storage.nullable<ui64>>, !db.column<!storage.path>, !db.column<!storage.nullable<!storage.string>>}
  } : (!db.column<!storage.path>) -> (!db.column<!storage.path>, !db.column<!storage.nullable<ui64>>, !db.column<!storage.path>, !db.column<!storage.nullable<!storage.string>>)
  %length = db.path_elements(%c#2) kind length : (!db.column<!storage.path>) -> !db.column<!storage.nullable<ui64>>
  db.output(%c#1, %length, %c#3) : !db.column<!storage.nullable<ui64>>, !db.column<!storage.nullable<ui64>>, !db.column<!storage.nullable<!storage.string>>
  return
}
)mlir";

// COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name LIMIT 2 }
const char* const countSubqueryProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %name = db.get_node_properties(%p, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
  %c = db.count_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    %s, %e, %et, %i = db.get_out_edges_by_type(%pc, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %interest = db.get_node_properties(%i, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    %sorted:2 = db.sort(%i, %interest) keys [1] ascending [true] : (!db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>) -> (!db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>)
    %first = db.limit(%sorted#0) count 2 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    db.count_subquery_yield {%first} : {!db.column<!storage.node_id>}
  } : (!db.column<!storage.node_id>) -> !db.column<ui64>
  db.output(%name, %c) : !db.column<!storage.nullable<!storage.string>>, !db.column<ui64>
  return
}
)mlir";

// EXISTS { MATCH (p)-[:KNOWS_WELL]->(q) RETURN q ORDER BY q.name LIMIT 1 }
const char* const existsSubqueryProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %name = db.get_node_properties(%p, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
  %found = db.exists_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    %s, %e, %et, %q = db.get_out_edges_by_type(%pc, ["KNOWS_WELL"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %qname = db.get_node_properties(%q, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    %sorted:2 = db.sort(%q, %qname) keys [1] ascending [true] : (!db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>) -> (!db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>)
    %first = db.limit(%sorted#0) count 1 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    db.exists_yield {%first} : {!db.column<!storage.node_id>}
  } : (!db.column<!storage.node_id>) -> !db.column<!storage.bool>
  db.output(%name, %found) : !db.column<!storage.nullable<!storage.string>>, !db.column<!storage.bool>
  return
}
)mlir";

// COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN p.name + '/' + i.name ORDER BY i.name }:
// the name of the constant is read once per run and laid out over the sorted rows
const char* const collectSubqueryProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %list = db.collect_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    %s, %e, %et, %i = db.get_out_edges_by_type(%pc, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %pname = db.get_node_properties(%pc, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    %slash = db.constant("/" : !storage.string)
    %prefix = db.concat %pname, %slash : (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.string>) -> !db.column<none>
    %iname = db.get_node_properties(%i, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    %pair = db.concat %prefix, %iname : (!db.column<none>, !db.column<!storage.nullable<!storage.string>>) -> !db.column<none>
    %sorted:2 = db.sort(%pair, %iname) keys [1] ascending [true] : (!db.column<none>, !db.column<!storage.nullable<!storage.string>>) -> (!db.column<none>, !db.column<!storage.nullable<!storage.string>>)
    db.collect_subquery_yield %sorted#0, {%sorted#0} : !db.column<none>, {!db.column<none>}
  } : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%list) : !db.column<none>
  return
}
)mlir";

// A body run once per row reads its arguments through db.row_constant, not as columns
const char* const directArgumentProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %r:2 = db.call_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %name = db.get_node_properties(%pIn, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    db.subquery_yield {%name} : {!db.column<!storage.nullable<!storage.string>>}
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>)
  db.output(%r#1) : !db.column<!storage.nullable<!storage.string>>
  return
}
)mlir";

// The same of a COUNT body
const char* const directCountArgumentProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %c = db.count_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %s, %e, %et, %i = db.get_out_edges_by_type(%pIn, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %first = db.limit(%i) count 1 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    db.count_subquery_yield {%first} : {!db.column<!storage.node_id>}
  } : (!db.column<!storage.node_id>) -> !db.column<ui64>
  db.output(%c) : !db.column<ui64>
  return
}
)mlir";

// The eight people a MATCH (x:Person) in a body walks, in sorted order
const char* const personNames[] = {"Adam", "Cyrus", "Doruk", "Luc", "Martina", "Maxime", "Remy", "Suhas"};

// A body that carries the scope runs over a chunk of rows, so its arguments are columns
const char* const carriedScopeProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %r:2 = db.call_subquery(%p) carries_scope {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %pc = db.row_constant %pIn : !db.column<!storage.node_id>
    db.subquery_yield {%pIn, %pc} : {!db.column<!storage.node_id>, !db.column<!storage.node_id>}
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%r#1) : !db.column<!storage.node_id>
  return
}
)mlir";

// A column the body computed is no argument of it
const char* const computedOperandProgram = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %r:2 = db.call_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %x = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    %xc = db.row_constant %x : !db.column<!storage.node_id>
    db.subquery_yield {%xc} : {!db.column<!storage.node_id>}
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%r#1) : !db.column<!storage.node_id>
  return
}
)mlir";

}

// db.row_constant reads an argument of a body run once per row as a constant: a node, an
// edge or a path the body reads, hops from, compares, folds or returns. simpledb's eight
// people have 15 INTERESTED_IN edges, and 3 KNOWS_WELL edges start at Remy, Adam and Ghosts.
class RowConstantTest : public TuringTest {
protected:
    void initialize() override {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    void runProgram(const char* programText, size_t chunkSize, RowSink& sink) {
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = parse(programText);
        ASSERT_TRUE(dbModule);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));

        DBLowering lowering(&_context, &view);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, chunkSize);
        interpreter.run();
    }

    void expectRows(const char* programText, const Rows& expected) {
        for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
            RowSink sink;
            runProgram(programText, chunkSize, sink);

            Rows rows;
            sink.sortedRows(rows);

            EXPECT_EQ(rows, expected) << "chunk size " << chunkSize;
        }
    }

    mlir::MLIRContext _context;
    std::unique_ptr<Graph> _graph;
};

TEST_F(RowConstantTest, readsAPropertyOfTheConstantOncePerRun) {
    expectRows(propertyProgram, {
        {"Adam"},
        {"Cyrus"},
        {"Doruk"},
        {"Luc"},
        {"Martina"},
        {"Maxime"},
        {"Remy"},
        {"Suhas"},
    });
}

TEST_F(RowConstantTest, hopsFromTheConstantAndCountsUnderAConstantKey) {
    expectRows(countProgram, {
        {"Adam", "2"},
        {"Cyrus", "2"},
        {"Doruk", "1"},
        {"Luc", "2"},
        {"Martina", "1"},
        {"Maxime", "2"},
        {"Remy", "3"},
        {"Suhas", "2"},
    });
}

TEST_F(RowConstantTest, makesNoGroupForAPersonWithNoEdge) {
    expectRows(emptyGroupProgram, {
        {"Adam", "1"},
        {"Remy", "1"},
    });
}

TEST_F(RowConstantTest, laysAValueOfTheConstantOverTheRowsOfTheHop) {
    expectRows(valueBesideRowsProgram, {
        {"Adam", "Bio"},
        {"Adam", "Cooking"},
        {"Cyrus", "Gym"},
        {"Cyrus", "Travel"},
        {"Doruk", "Gym"},
        {"Luc", "Animals"},
        {"Luc", "Computers"},
        {"Martina", "Cooking"},
        {"Maxime", "Bio"},
        {"Maxime", "Padel"},
        {"Remy", "Computers"},
        {"Remy", "Eighties"},
        {"Remy", "Ghosts"},
        {"Suhas", "Gym"},
        {"Suhas", "JiuJitsu"},
    });
}

TEST_F(RowConstantTest, laysTheConstantNodeOverTheRowsOfTheHop) {
    expectRows(nodeBesideRowsProgram, {
        {"Adam", "Bio"},
        {"Adam", "Cooking"},
        {"Cyrus", "Gym"},
        {"Cyrus", "Travel"},
        {"Doruk", "Gym"},
        {"Luc", "Animals"},
        {"Luc", "Computers"},
        {"Martina", "Cooking"},
        {"Maxime", "Bio"},
        {"Maxime", "Padel"},
        {"Remy", "Computers"},
        {"Remy", "Eighties"},
        {"Remy", "Ghosts"},
        {"Suhas", "Gym"},
        {"Suhas", "JiuJitsu"},
    });
}

TEST_F(RowConstantTest, hopsFromTheConstantLaidOverTheRowsInFlight) {
    expectRows(hopOverRowsProgram, {
        {"Adam", "16"},
        {"Cyrus", "16"},
        {"Doruk", "8"},
        {"Luc", "16"},
        {"Martina", "8"},
        {"Maxime", "16"},
        {"Remy", "24"},
        {"Suhas", "16"},
    });
}

TEST_F(RowConstantTest, comparesTheConstantWithEachRow) {
    expectRows(equalityProgram, {
        {"Adam", "Adam"},
        {"Cyrus", "Cyrus"},
        {"Doruk", "Doruk"},
        {"Luc", "Luc"},
        {"Martina", "Martina"},
        {"Maxime", "Maxime"},
        {"Remy", "Remy"},
        {"Suhas", "Suhas"},
    });
}

TEST_F(RowConstantTest, countsTheConstantNodeOverTheRowsInFlight) {
    expectRows(countEntityProgram, Rows(8, Row {"8"}));
}

TEST_F(RowConstantTest, collectsTheConstantNodeOverTheRowsInFlight) {
    expectRows(collectEntityProgram, Rows(8, Row {"8"}));
}

TEST_F(RowConstantTest, readsAndLaysOutAConstantEdge) {
    Rows expected;
    for (const char* source : {"0", "1", "6"}) {
        for (const char* name : personNames) {
            expected.push_back({"KNOWS_WELL", source, name});
        }
    }

    expectRows(edgeProgram, expected);
}

TEST_F(RowConstantTest, readsAndLaysOutAConstantPath) {
    Rows expected;
    for (size_t pathIndex = 0; pathIndex < 3; pathIndex++) {
        for (const char* name : personNames) {
            expected.push_back({"1", "1", name});
        }
    }

    std::sort(expected.begin(), expected.end());
    expectRows(pathProgram, expected);
}

TEST_F(RowConstantTest, countsTheRowsOfABodyHoppingFromTheConstant) {
    expectRows(countSubqueryProgram, {
        {"Adam", "2"},
        {"Cyrus", "2"},
        {"Doruk", "1"},
        {"Luc", "2"},
        {"Martina", "1"},
        {"Maxime", "2"},
        {"Remy", "2"},
        {"Suhas", "2"},
    });
}

TEST_F(RowConstantTest, findsTheRowsOfABodyHoppingFromTheConstant) {
    expectRows(existsSubqueryProgram, {
        {"Adam", "true"},
        {"Cyrus", "false"},
        {"Doruk", "false"},
        {"Luc", "false"},
        {"Martina", "false"},
        {"Maxime", "false"},
        {"Remy", "true"},
        {"Suhas", "false"},
    });
}

TEST_F(RowConstantTest, collectsAValueOfTheConstantBesideTheRowsOfTheBody) {
    expectRows(collectSubqueryProgram, {
        {"[Adam/Bio, Adam/Cooking]"},
        {"[Cyrus/Gym, Cyrus/Travel]"},
        {"[Doruk/Gym]"},
        {"[Luc/Animals, Luc/Computers]"},
        {"[Martina/Cooking]"},
        {"[Maxime/Bio, Maxime/Padel]"},
        {"[Remy/Computers, Remy/Eighties, Remy/Ghosts]"},
        {"[Suhas/Gym, Suhas/JiuJitsu]"},
    });
}

TEST_F(RowConstantTest, rejectsAnArgumentOfABodyCarryingTheScope) {
    EXPECT_FALSE(parse(carriedScopeProgram));
}

TEST_F(RowConstantTest, rejectsAColumnTheBodyComputed) {
    EXPECT_FALSE(parse(computedOperandProgram));
}

TEST_F(RowConstantTest, rejectsABodyRunOncePerRowReadingAnArgumentAsAColumn) {
    EXPECT_FALSE(parse(directArgumentProgram));
    EXPECT_FALSE(parse(directCountArgumentProgram));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
