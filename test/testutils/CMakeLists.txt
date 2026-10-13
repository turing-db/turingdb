add_library(turing_testmain_obj OBJECT TuringTestMain.cpp)

target_link_libraries(turing_testmain_obj
    PRIVATE GTest::gtest
            turing_common_s
            turing_db_jobs_s
            spdlog)

add_library(turing_testutils_s STATIC RemoteTestUtils.cpp)

target_link_libraries(turing_testutils_s
    PUBLIC  GTest::gtest
            GTest::gmock
            turing_common_s
            turing_db_jobs_s
            spdlog
    PRIVATE nlohmann_json)

target_include_directories(turing_testutils_s
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})

# Every library a test may call into. A test links turing_test_libs and nothing else,
# so no test ever holds a second copy of a library next to this one.
set(TURING_TEST_LIBS
    turing_testutils_s
    turing_testenv_s
    turing_ir_test_rows_s
    turing_ir_test_write_query_s
    turing_ir_test_hash_join_s
    turing_common_s
    turing_db_s
    turing_db_analyzer_s
    turing_db_ast_s
    turing_db_auth_s
    turing_db_decoders_s
    turing_db_examples_s
    turing_db_frontend_cypher_s
    turing_db_http_common_s
    turing_db_import_gml_s
    turing_db_interpreter_s
    turing_db_io_cache_s
    turing_db_io_filecache_s
    turing_db_io_fs_s
    turing_db_io_parquet_s
    turing_db_io_s3_s
    turing_db_ir_codegen_s
    turing_db_ir_common_s
    turing_db_ir_dbdialect_s
    turing_db_ir_exec_s
    turing_db_ir_lowering_s
    turing_db_ir_nldialect_s
    turing_db_ir_storagedialect_s
    turing_db_jobs_s
    turing_db_memory_s
    turing_db_net_encoders
    turing_db_parser_s
    turing_db_procedures_s
    turing_db_proto_client_s
    turing_db_proto_common_s
    turing_db_query_common_s
    turing_db_server_s
    turing_db_storage_s
    turing_db_system_s
    turing_db_turing_proto_server_s
    turing_vector_s
    linenoise_s
    MLIRParser
    Parquet::parquet_static
    GTest::gtest
    GTest::gmock)

# Header-only libraries the tests include directly; they have no archive to fold in.
set(TURING_TEST_HEADER_LIBS
    turing_db_http_parser_s
    nlohmann_json)

# On Linux the test libraries are folded whole into one shared library, so a test
# binary holds only its own code instead of a 146 MB copy of the engine and MLIR.
# macOS links libc++ statically and hidden (see the root CMakeLists.txt), so a shared
# library there would carry a second libc++; it keeps the static link.
if(APPLE)
    add_library(turing_test_libs STATIC $<TARGET_OBJECTS:turing_testmain_obj>)
    target_link_libraries(turing_test_libs PUBLIC ${TURING_TEST_LIBS} ${TURING_TEST_HEADER_LIBS})
else()
    if(CMAKE_VERSION VERSION_LESS 3.27)
        message(FATAL_ERROR "The shared test library needs CMake 3.27 or newer, found ${CMAKE_VERSION}")
    endif()

    # The archives go in once, as one whole-archive group, and the targets are linked
    # normally for their dependencies: CMake repeats archives to settle cycles between
    # static libraries, and a repeat under --whole-archive defines every symbol twice.
    add_library(turing_test_libs SHARED $<TARGET_OBJECTS:turing_testmain_obj>)
    target_link_libraries(turing_test_libs PRIVATE ${TURING_TEST_LIBS})

    set(TURING_TEST_LIB_FILES "")
    foreach(lib ${TURING_TEST_LIBS})
        list(APPEND TURING_TEST_LIB_FILES "$<TARGET_FILE:${lib}>")
    endforeach()

    foreach(lib ${TURING_TEST_LIBS} ${TURING_TEST_HEADER_LIBS})
        target_link_libraries(turing_test_libs INTERFACE "$<COMPILE_ONLY:${lib}>")
    endforeach()

    target_link_options(turing_test_libs
        PRIVATE -Wl,--whole-archive ${TURING_TEST_LIB_FILES} -Wl,--no-whole-archive)
endif()

# One ctest test per binary, not per case. gtest_discover_tests would report each
# case separately, at the cost of relaunching the binary once per case: every launch
# stands up a fresh SystemManager whose JobSystem costs CPU linear in core count.
function(add_turing_test exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE turing_test_libs)

    add_test(NAME ${exe_name} COMMAND ${exe_name})
    set_tests_properties(${exe_name} PROPERTIES
        ENVIRONMENT "TURING_EXTENSIONS_DIR=${CMAKE_BINARY_DIR}/lib/turingdb/extensions;OPENBLAS_NUM_THREADS=1")
endfunction()
