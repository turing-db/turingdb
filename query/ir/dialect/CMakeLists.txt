set(STORAGE_DIALECT_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/storage CACHE INTERNAL "")
set(STORAGE_DIALECT_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/storage CACHE INTERNAL "")

add_subdirectory(storage)
add_subdirectory(db)
add_subdirectory(nl)
