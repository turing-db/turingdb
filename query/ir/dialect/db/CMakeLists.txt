set(LLVM_TARGET_DEFINITIONS DBOps.td)

mlir_tablegen(DBOps.h.inc   -gen-op-decls)
mlir_tablegen(DBOps.cpp.inc -gen-op-defs)
mlir_tablegen(DBDialect.h.inc      -gen-dialect-decls)
mlir_tablegen(DBDialect.cpp.inc    -gen-dialect-defs)

add_public_tablegen_target(DBDialectIncGen)

add_mlir_library(DBDialect
  DBDialect.cpp
  DEPENDS DBDialectIncGen
  LINK_LIBS PUBLIC MLIRIR MLIRSupport
)


target_include_directories(DBDialect PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
)
