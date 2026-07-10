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
// AggregateKind by name. It is stored on the ops as a DenseI64ArrayAttr of these
// integer values (as db.sort stores its key column indices), so no enum-array
// attribute machinery is needed; the values are range-checked in the verifiers.
def GroupAggregateKind : I64EnumAttr<"GroupAggregateKind", "A grouped aggregate function", [
    I64EnumAttrCase<"Count", 0, "count">,
    I64EnumAttrCase<"Sum", 1, "sum">,
    I64EnumAttrCase<"Min", 2, "min">,
    I64EnumAttrCase<"Max", 3, "max">,
    I64EnumAttrCase<"Avg", 4, "avg">,
]> {
    let cppNamespace = "::mlir::storage";
}

#endif // STORAGE_ENUMS
