# Set the root tablegen file of all subsequent calls to
# `mlir_tablegen` to be DBDialect.td
set(LLVM_TARGET_DEFINITIONS DBDialect.td)

# Perform codegen from TableGen
#             Generated file     File type          Current dialect  Add needed mlir headers
mlir_tablegen(DBDialect.h.inc    -gen-dialect-decls -dialect db  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})
mlir_tablegen(DBDialect.cpp.inc  -gen-dialect-defs  -dialect db  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})

set(LLVM_TARGET_DEFINITIONS DBInterfaces.td)
mlir_tablegen(DBInterfaces.h.inc   -gen-op-interface-decls  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})
mlir_tablegen(DBInterfaces.cpp.inc -gen-op-interface-defs   EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})

set(LLVM_TARGET_DEFINITIONS DBOps.td)
mlir_tablegen(DBOps.h.inc        -gen-op-decls      -dialect db  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS} ${STORAGE_DIALECT_SOURCE_DIR})
mlir_tablegen(DBOps.cpp.inc      -gen-op-defs       -dialect db  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS} ${STORAGE_DIALECT_SOURCE_DIR})

set(LLVM_TARGET_DEFINITIONS DBTypes.td)
mlir_tablegen(DBTypes.h.inc      -gen-typedef-decls     -dialect db --typedefs-dialect db EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS} ${STORAGE_DIALECT_SOURCE_DIR})
mlir_tablegen(DBTypes.cpp.inc    -gen-typedef-defs      -dialect db --typedefs-dialect db EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS} ${STORAGE_DIALECT_SOURCE_DIR})

set (LLVM_TARGET_DEFINITIONS DBPasses.td)
mlir_tablegen(DBPasses.h.inc     -gen-pass-decls    -name DB -dialect db  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})

# Creates CMAKE target with all included .inc files, the lib depends on this
add_public_tablegen_target(DBDialectIncGen)

set_source_files_properties(
  DBAggregateOps.cpp
  DBColumnChecks.cpp
  DBDialect.cpp
  DBExpressionOps.cpp
  DBFunctionOps.cpp
  DBHopOps.cpp
  DBJoinOps.cpp
  DBListOps.cpp
  DBOps.cpp
  DBPathOps.cpp
  DBPropertyOps.cpp
  DBRegionColumns.cpp
  DBRowOps.cpp
  DBScanOps.cpp
  DBSourceOps.cpp
  DBSpeculation.cpp
  DBSubqueryOps.cpp
  DBTypes.cpp
  PROPERTIES COMPILE_FLAGS "-Wno-uninitialized -Wno-ambiguous-reversed-operator"
)

# Compiles .cpp files; needs the codegen first, hence the DEPENDS clause
add_mlir_library(turing_db_ir_dbdialect_s
  DBCanonicalizePasses.cpp
  DBCarriedColumns.cpp
  DBColumnElement.cpp
  DBCrossProducts.cpp
  DBDialect.cpp
  DBExplorePasses.cpp
  DBFactorPasses.cpp
  DBFilterPasses.cpp
  DBFilterUtils.cpp
  DBHeldTags.cpp
  DBInterfaces.cpp
  DBJoinPasses.cpp
  DBKnownLabels.cpp
  DBLabelAndTypePasses.cpp
  DBLineageClimbs.cpp
  DBLookupPredicates.cpp
  DBOps.cpp
  DBAggregateOps.cpp
  DBColumnChecks.cpp
  DBExpressionOps.cpp
  DBFunctionOps.cpp
  DBHopOps.cpp
  DBJoinOps.cpp
  DBListOps.cpp
  DBPathOps.cpp
  DBPropertyOps.cpp
  DBRegionColumns.cpp
  DBRowOps.cpp
  DBScanOps.cpp
  DBSourceOps.cpp
  DBSpeculation.cpp
  DBSubqueryOps.cpp
  DBTypes.cpp
  DBPassUtils.cpp
  DBPathPasses.cpp
  DBPatternWalk.cpp
  DBReadReusePasses.cpp
  DBRerootPasses.cpp
  DBRowCone.cpp
  DBRowEstimator.cpp
  DBScanFusionPasses.cpp
  DBStagePlanner.cpp
  DBTrimPasses.cpp
  DBUnwindEqualityCross.cpp
  DBUnwindPasses.cpp
  DBWrittenOrder.cpp

  DEPENDS
  DBDialectIncGen

  LINK_LIBS PUBLIC
  MLIRIR
  MLIRSupport
  MLIRPass
  MLIRTransformUtils
  MLIRFuncDialect
  MLIRBuiltinToLLVMIRTranslation
  turing_db_ir_storagedialect_s
  turing_db_ir_common_s

  # The join cost model estimates each side from the graph's node counts, which is what
  # the v2 planner reads it from too
  turing_db_query_common_s
)

# Ensure we have MLIR and LLVM includes, as well as the headers in current dir
target_include_directories(turing_db_ir_dbdialect_s PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${STORAGE_DIALECT_BINARY_DIR}
  ${IR_COMMON_SOURCE_DIR}
  ${MLIR_INCLUDE_DIRS}
  ${LLVM_INCLUDE_DIRS}
)
