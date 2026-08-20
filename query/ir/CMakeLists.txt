set(IR_COMMON_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/common CACHE INTERNAL "")

add_subdirectory(dialect)
add_subdirectory(common)
add_subdirectory(as)
add_subdirectory(interpreter)
add_subdirectory(lowering)
add_subdirectory(frontend)
add_subdirectory(codegen)
