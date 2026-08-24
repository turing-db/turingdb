#ifndef STORAGE_ENUMS
#define STORAGE_ENUMS

include "mlir/IR/EnumAttr.td"

// The reduction an aggregate applies to a column of property values. It is used
// by the nl dialect (the three nl.aggregate ops carry it) and set by the lowering
// from each db aggregate op (db.sum / db.min / db.max / db.avg), so it lives in
// the storage dialect the way the value/nullable types do, rather than being
// defined in a single dialect.
//
// COUNT is deliberately not a case here: it tallies rows regardless of value and
// has its own op family (db.count, nl.count*). These four all reduce the values
// themselves and share the create/update/result shape, so one enum drives them.
def AggregateKind : I64EnumAttr<"AggregateKind", "A property-value aggregate", [
    I64EnumAttrCase<"Sum", 0, "sum">,
    I64EnumAttrCase<"Min", 1, "min">,
    I64EnumAttrCase<"Max", 2, "max">,
    I64EnumAttrCase<"Avg", 3, "avg">,
]> {
    let cppNamespace = "::mlir::storage";
}

// The reduction one function of a grouped aggregation applies. Unlike
// AggregateKind it includes COUNT: a grouped aggregate carries a list of
// functions (db.group_aggregate's `kinds`), and count is one of them - it
// tallies each group's non-null rows the same way sum/min/max/avg reduce their
// values, so the four value reductions and the row tally share one enum here.
//
// The cases keep AggregateKind's order for the four value reductions, with COUNT
// prepended at 0, so a GroupAggregateKind other than Count maps onto the matching
// AggregateKind by name. It is stored on the ops as a compact DenseI64ArrayAttr of
// these integer values (as db.sort stores its key column indices), but printed and
// parsed as the symbolic keywords through the shared custom<GroupAggregateKinds>
// assembly directive (GroupAggregateKindsFormat), which reuses the generated
// symbolize/stringify below; the values are also range-checked in the verifiers.
//
// COUNT_DISTINCT is Cypher's count(DISTINCT x): it tallies each group's distinct
// non-null values rather than its non-null rows, and emits the same ui64 per group
// as COUNT. It is its own kind rather than a modifier on COUNT so the kinds array
// stays the whole aggregate spec - a grouped aggregate needs no second attribute
// parallel to it. The scalar count(DISTINCT x) is db.count's `distinct` flag
// instead: with no group to key on, it is the plain tally of a deduplicated column.
//
// COUNT_ROWS is Cypher's count(*): it tallies every row of a group rather than the
// rows whose value is present, so it reads no value at all and accepts a column of any
// type - which is what lets count(*) be anchored on whichever column of the part carries
// its rows, a nullable property column included. COUNT is count(x) and keeps its
// null-skipping tally.
//
// SUM_DISTINCT and AVG_DISTINCT are the same for Cypher's sum(DISTINCT x) and
// avg(DISTINCT x): they reduce each group's distinct non-null values and keep the
// result shape of SUM and AVG. MIN and MAX need no such kind - dropping repeated
// values cannot move an extremum, so min(DISTINCT x) is min(x) and the frontend
// emits the plain kind.
def GroupAggregateKind : I64EnumAttr<"GroupAggregateKind", "A grouped aggregate function", [
    I64EnumAttrCase<"Count", 0, "count">,
    I64EnumAttrCase<"Sum", 1, "sum">,
    I64EnumAttrCase<"Min", 2, "min">,
    I64EnumAttrCase<"Max", 3, "max">,
    I64EnumAttrCase<"Avg", 4, "avg">,
    I64EnumAttrCase<"CountDistinct", 5, "count_distinct">,
    I64EnumAttrCase<"SumDistinct", 6, "sum_distinct">,
    I64EnumAttrCase<"AvgDistinct", 7, "avg_distinct">,
    I64EnumAttrCase<"CountRows", 8, "count_rows">,
]> {
    let cppNamespace = "::mlir::storage";
}

#endif // STORAGE_ENUMS
