# Set the root tablegen file of all subsequent calls to
# `mlir_tablegen` to be DBDialect.td
set(LLVM_TARGET_DEFINITIONS DBDialect.td)

# Perform codegen from TableGen
#             Generated file     File type          Current dialect  Add needed mlir headers
mlir_tablegen(DBDialect.h.inc    -gen-dialect-decls -dialect db  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})
mlir_tablegen(DBDialect.cpp.inc  -gen-dialect-defs  -dialect db  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})
 
set(LLVM_TARGET_DEFINITIONS DBOps.td)
mlir_tablegen(DBOps.h.inc        -gen-op-decls      -dialect db  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})
mlir_tablegen(DBOps.cpp.inc      -gen-op-defs       -dialect db  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})

set(LLVM_TARGET_DEFINITIONS DBTypes.td)
mlir_tablegen(DBTypes.h.inc      -gen-typedef-decls     -dialect db EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})
mlir_tablegen(DBTypes.cpp.inc    -gen-typedef-defs      -dialect db EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})

# Creates CMAKE target with all included .inc files, the lib depends on this
add_public_tablegen_target(DBDialectIncGen)

set_source_files_properties(
  DBDialect.cpp
  DBOps.cpp
  DBTypes.cpp
  PROPERTIES COMPILE_FLAGS "-Wno-uninitialized"
)

# Compiles .cpp files; needs the codegen first, hence the DEPENDS clause
add_mlir_library(DBDialect
  DBDialect.cpp
  DBOps.cpp
  DBTypes.cpp

  DEPENDS
  DBDialectIncGen

  LINK_LIBS PUBLIC
  MLIRIR
  MLIRSupport
  MLIRFuncDialect
  MLIRBuiltinToLLVMIRTranslation
)

# Ensure we have MLIR and LLVM includes, as well as the headers in current dir
target_include_directories(DBDialect PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  $<BUILD_INTERFACE:${MLIR_INCLUDE_DIRS}>
  $<BUILD_INTERFACE:${LLVM_INCLUDE_DIRS}>
)
