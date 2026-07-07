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

#endif // STORAGE_ENUMS
