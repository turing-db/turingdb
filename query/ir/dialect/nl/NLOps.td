#ifndef TURINGDB_NL_OPS
#define TURINGDB_NL_OPS

include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"
include "NLSystemOps.td"
include "NLScanOps.td"
include "NLHopOps.td"
include "NLPathOps.td"
include "NLSourceOps.td"
include "NLPropertyOps.td"
include "NLListOps.td"
include "NLExpressionOps.td"
include "NLFunctionOps.td"
include "NLRowOps.td"
include "NLJoinOps.td"
include "NLAggregateOps.td"
include "NLSubqueryOps.td"
include "NLWriteOps.td"

#endif //TURINGDB_NL_OPS
