set(LLVM_TARGET_DEFINITIONS StorageDialect.td)

mlir_tablegen(StorageDialect.h.inc   -gen-dialect-decls -dialect storage  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})
mlir_tablegen(StorageDialect.cpp.inc -gen-dialect-defs  -dialect storage  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})

set(LLVM_TARGET_DEFINITIONS StorageTypes.td)
mlir_tablegen(StorageTypes.h.inc     -gen-typedef-decls -dialect storage  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})
mlir_tablegen(StorageTypes.cpp.inc   -gen-typedef-defs  -dialect storage  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})

set(LLVM_TARGET_DEFINITIONS StorageEnums.td)
mlir_tablegen(StorageEnums.h.inc     -gen-enum-decls    EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})
mlir_tablegen(StorageEnums.cpp.inc   -gen-enum-defs     EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})

add_public_tablegen_target(StorageDialectIncGen)

set_source_files_properties(
  StorageDialect.cpp
  StorageTypes.cpp
  PROPERTIES COMPILE_FLAGS "-Wno-uninitialized"
)

add_mlir_library(turing_db_ir_storagedialect_s
  StorageDialect.cpp
  StorageTypes.cpp
  GroupAggregateKindsFormat.cpp
  EdgeDirectionsFormat.cpp
  MergePatternShape.cpp
  ColumnIndicesFormat.cpp

  DEPENDS
  StorageDialectIncGen

  LINK_LIBS PUBLIC
  MLIRIR
  MLIRSupport
  MLIRFuncDialect
  MLIRBuiltinToLLVMIRTranslation
)

target_include_directories(turing_db_ir_storagedialect_s PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${MLIR_INCLUDE_DIRS}
  ${LLVM_INCLUDE_DIRS}
)
