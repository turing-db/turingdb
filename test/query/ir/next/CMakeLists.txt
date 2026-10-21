# Queries chained with NEXT.
add_turing_test(test_query_ir_next_query NextQueryTest.cpp)

# NEXT chains whose segments write.
add_turing_test(test_query_ir_next_write NextWriteTest.cpp)

# The segment after one that writes over a table starts from one row.
add_turing_test(test_query_ir_next_after_writing_segment NextAfterWritingSegmentTest.cpp)

# The segment after a WHEN whose branches only write starts from one row.
add_turing_test(test_query_ir_next_when_that_writes NextWhenThatWritesTest.cpp)

# A CALL's imports stay readable across the segments of a chain in its body.
add_turing_test(test_query_ir_next_keeps_call_imports NextKeepsCallImportsTest.cpp)

# A chain in an unscoped CALL body opens on a RETURN, not an importing WITH.
add_turing_test(test_query_ir_next_in_unscoped_call NextInUnscopedCallTest.cpp)

# A CALL's imports stay readable after a segment of its body that writes.
add_turing_test(test_query_ir_next_keeps_call_imports_after_write NextKeepsCallImportsAfterWriteTest.cpp)

# A CALL's import read after a UNION segment whose branch projects through a WITH.
add_turing_test(test_query_ir_next_import_past_branch_with NextImportPastBranchWithTest.cpp)
