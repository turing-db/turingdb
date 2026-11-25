#ifndef TURINGDB_OPS
#define TURINGDB_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/IR/OpAsmInterface.td"
include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"
include "DBSystemOps.td"
include "DBScanOps.td"
include "DBHopOps.td"
include "DBPathOps.td"
include "DBSourceOps.td"
include "DBPropertyOps.td"
include "DBListOps.td"
include "DBFunctionOps.td"
include "DBExpressionOps.td"
include "DBRowOps.td"
include "DBJoinOps.td"
include "DBAggregateOps.td"
include "DBSubqueryOps.td"
include "DBWriteOps.td"

#endif //TURINGDB_OPS
