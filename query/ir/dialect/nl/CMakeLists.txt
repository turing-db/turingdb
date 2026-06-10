# Set the root tablegen file of all subsequent calls to
# `mlir_tablegen` to be NLDialect.td
set(LLVM_TARGET_DEFINITIONS NLDialect.td)

# Perform codegen from TableGen
#             Generated file     File type          Current dialect  Add needed mlir headers
mlir_tablegen(NLDialect.h.inc    -gen-dialect-decls -dialect nl  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})
mlir_tablegen(NLDialect.cpp.inc  -gen-dialect-defs  -dialect nl  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})

set(LLVM_TARGET_DEFINITIONS NLOps.td)
mlir_tablegen(NLOps.h.inc        -gen-op-decls      -dialect nl  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})
mlir_tablegen(NLOps.cpp.inc      -gen-op-defs       -dialect nl  EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})

set(LLVM_TARGET_DEFINITIONS NLTypes.td)
mlir_tablegen(NLTypes.h.inc      -gen-typedef-decls     -dialect nl EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})
mlir_tablegen(NLTypes.cpp.inc    -gen-typedef-defs      -dialect nl EXTRA_INCLUDES ${MLIR_INCLUDE_DIRS})

# Creates CMAKE target with all included .inc files, the lib depends on this
add_public_tablegen_target(NLDialectIncGen)

set_source_files_properties(
  NLDialect.cpp
  NLOps.cpp
  NLTypes.cpp
  PROPERTIES COMPILE_FLAGS "-Wno-uninitialized"
)

# Compiles .cpp files; needs the codegen first, hence the DEPENDS clause
add_mlir_library(turing_db_ir_nldialect_s
  NLDialect.cpp
  NLOps.cpp
  NLTypes.cpp

  DEPENDS
  NLDialectIncGen

  LINK_LIBS PUBLIC
  MLIRIR
  MLIRSupport
  MLIRFuncDialect
  MLIRBuiltinToLLVMIRTranslation
)

# Ensure we have MLIR and LLVM includes, as well as the headers in current dir
target_include_directories(turing_db_ir_nldialect_s PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${MLIR_INCLUDE_DIRS}
  ${LLVM_INCLUDE_DIRS}
)
