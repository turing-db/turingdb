function(add_ir_tests exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE
            turing_common_s
            turing_db_storage_s
            turing_db_jobs_s
            turing_db_ir_interpreter_s
            turing_db_ir_lowering_s
            turing_db_ir_dbdialect_s
            turing_db_ir_common_s
            MLIRParser)
endfunction()

add_ir_tests(test_query_ir_nlexecutor NLExecutorTest.cpp)
add_ir_tests(test_query_ir_dblowering DBLoweringTest.cpp)
add_ir_tests(test_query_ir_dbdialect DBDialectTest.cpp)
add_ir_tests(test_query_ir_nldialect NLDialectTest.cpp)
