
set(as_sources IRAssembler.cpp)

add_library(turing_db_ir_assembler_s STATIC ${as_sources})

target_link_libraries(turing_db_ir_assembler_s
    PRIVATE
        turing_db_ir_dbdialect_s
        turing_db_ir_common_s
        turing_db_io_fs_s
        turing_common_s)

target_include_directories(turing_db_ir_assembler_s PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR})
