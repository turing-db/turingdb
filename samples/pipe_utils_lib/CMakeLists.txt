set(sources PipeUtils.cpp)

add_library(turing_samples_pipe_utilslib_s STATIC  ${sources})

target_include_directories(turing_samples_pipe_utilslib_s
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_samples_pipe_utilslib_s
    PUBLIC  common_s
            turing_db2_system_s
            turing_db2_core_s
            turing_db2_import_neo4j_s
            turing_db2_query_interpreter_s
            turing_db2_query_plan_s
            turing_db2_server_s
            turing_log_s)
