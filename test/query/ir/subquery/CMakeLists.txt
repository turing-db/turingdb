# CALL { ... } subqueries whose body carries the rows it is given through to its RETURN.
add_turing_test(test_query_ir_call_subquery CallSubqueryTest.cpp)
target_include_directories(test_query_ir_call_subquery PRIVATE ${IR_TEST_DIR})

# Unit subqueries: a body ending on an updating clause, run once per row in flight.
add_turing_test(test_query_ir_call_subquery_write CallSubqueryWriteTest.cpp)

# CALL (*) { ... }: a body importing every variable in scope.
add_turing_test(test_query_ir_call_subquery_import_everything CallSubqueryImportEverythingTest.cpp)

# Subqueries whose body aggregates, sorts, cuts or dedups, run one input row at a time.
add_turing_test(test_query_ir_call_subquery_per_row CallSubqueryPerRowTest.cpp)
target_include_directories(test_query_ir_call_subquery_per_row PRIVATE ${IR_TEST_DIR})

# Every example of the Neo4j manual's EXISTS subqueries page, over the graph that page
# builds, answering the rows it documents.
add_turing_test(test_query_ir_exists_neo4j_manual ExistsNeo4jManualTest.cpp)
target_include_directories(test_query_ir_exists_neo4j_manual PRIVATE ${IR_TEST_DIR})

# Which path each kind of EXISTS body compiles to: the tag the rows carry, or the loop
# that runs the body one input row at a time.
add_ir_tests(test_query_ir_exists_subquery_codegen ExistsSubqueryCodegenTest.cpp)
target_sources(test_query_ir_exists_subquery_codegen PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_exists_subquery_codegen PRIVATE ${IR_TEST_DIR})

# EXISTS { ... }: one boolean per row in flight, over the pattern shorthand and the
# MATCH body, correlated and not, and over a body that runs one input row at a time.
add_turing_test(test_query_ir_exists_subquery ExistsSubqueryTest.cpp)
target_include_directories(test_query_ir_exists_subquery PRIVATE ${IR_TEST_DIR})

# COUNT { ... }: one count per row in flight, over the pattern shorthand, the MATCH body, a
# body run one input row at a time, UNION and UNION ALL.
add_turing_test(test_query_ir_count_subquery CountSubqueryTest.cpp)

# Which path each kind of COUNT body compiles to.
add_ir_tests(test_query_ir_count_subquery_codegen CountSubqueryCodegenTest.cpp)
target_sources(test_query_ir_count_subquery_codegen PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_count_subquery_codegen PRIVATE ${IR_TEST_DIR})

# Every example of the Neo4j manual's COUNT subqueries page, over the graph that page
# builds, answering the rows it documents.
add_turing_test(test_query_ir_count_neo4j_manual CountNeo4jManualTest.cpp)
target_include_directories(test_query_ir_count_neo4j_manual PRIVATE ${IR_TEST_DIR})

# COLLECT { ... }: one list per row in flight, of what the body returns for it, over a body
# carrying the rows, one run a row at a time, UNION and UNION ALL.
add_turing_test(test_query_ir_collect_subquery CollectSubqueryTest.cpp)

# Which path each kind of COLLECT body compiles to.
add_ir_tests(test_query_ir_collect_subquery_codegen CollectSubqueryCodegenTest.cpp)
target_sources(test_query_ir_collect_subquery_codegen PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_collect_subquery_codegen PRIVATE ${IR_TEST_DIR})

# Every example of the Neo4j manual's COLLECT subqueries page, over the graph that page
# builds, answering the rows it documents.
add_turing_test(test_query_ir_collect_neo4j_manual CollectNeo4jManualTest.cpp)
target_include_directories(test_query_ir_collect_neo4j_manual PRIVATE ${IR_TEST_DIR})

# OPTIONAL CALL { ... }: the input rows the body yields nothing for, padded with null.
add_turing_test(test_query_ir_call_subquery_optional CallSubqueryOptionalTest.cpp)
target_include_directories(test_query_ir_call_subquery_optional PRIVATE ${IR_TEST_DIR})

# A body returning a boolean: the padded rows must read as null, not as false.
add_turing_test(test_query_ir_call_subquery_optional_bool CallSubqueryOptionalBoolTest.cpp)
target_include_directories(test_query_ir_call_subquery_optional_bool PRIVATE ${IR_TEST_DIR})

# A collect in a per-row subquery body whose folded column is bound above the row loop.
add_turing_test(test_query_ir_call_subquery_constant_collect CallSubqueryConstantCollectTest.cpp)
target_include_directories(test_query_ir_call_subquery_constant_collect PRIVATE ${IR_TEST_DIR})

# A procedure CALL ending a subquery body must not excuse the body from its RETURN.
add_turing_test(test_query_ir_call_subquery_procedure_body CallSubqueryProcedureBodyTest.cpp)
target_include_directories(test_query_ir_call_subquery_procedure_body PRIVATE ${IR_TEST_DIR})

# A CALL subquery body that writes and ends on a RETURN.
add_turing_test(test_query_ir_call_subquery_writing_return CallSubqueryWritingReturnTest.cpp)
target_include_directories(test_query_ir_call_subquery_writing_return PRIVATE ${IR_TEST_DIR})

# A procedure call in a subquery body forces the per-row strategy.
add_turing_test(test_query_ir_call_subquery_procedure_strategy CallSubqueryProcedureStrategyTest.cpp)
target_include_directories(test_query_ir_call_subquery_procedure_strategy PRIVATE ${IR_TEST_DIR})

# The block a subquery body roots in, when its inputs are bound in sibling blocks.
add_turing_test(test_query_ir_call_subquery_step_block CallSubqueryStepBlockTest.cpp)
target_include_directories(test_query_ir_call_subquery_step_block PRIVATE ${IR_TEST_DIR})

# The region op and the loops a CALL subquery compiles to, read off EXPLAIN.
add_ir_tests(test_query_ir_call_subquery_codegen CallSubqueryCodegenTest.cpp)
target_sources(test_query_ir_call_subquery_codegen PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_call_subquery_codegen PRIVATE ${IR_TEST_DIR})

# What a CALL subquery expresses per input row that the clauses around it cannot.
add_turing_test(test_query_ir_call_subquery_teams CallSubqueryTeamsTest.cpp)
target_include_directories(test_query_ir_call_subquery_teams PRIVATE ${IR_TEST_DIR})

# An aggregate or a cut inside a unit subquery body, which covers one input row.
add_turing_test(test_query_ir_call_subquery_unit_body_scope CallSubqueryUnitBodyScopeTest.cpp)

# OPTIONAL over a unit subquery body, which has no row to pad.
add_turing_test(test_query_ir_call_subquery_optional_unit CallSubqueryOptionalUnitTest.cpp)

# A reduction over a constant inside a subquery body, folded above the body it sits in.
add_turing_test(test_query_ir_call_subquery_constant_aggregate CallSubqueryConstantAggregateTest.cpp)
target_include_directories(test_query_ir_call_subquery_constant_aggregate PRIVATE ${IR_TEST_DIR})

# A subquery body that reads and returns nothing, and the error it is rejected with.
add_turing_test(test_query_ir_call_subquery_read_only_body CallSubqueryReadOnlyBodyTest.cpp)
target_include_directories(test_query_ir_call_subquery_read_only_body PRIVATE ${IR_TEST_DIR})

# A subquery inside a subquery, and what the inner body reads of the outer one.
add_turing_test(test_query_ir_call_subquery_nested CallSubqueryNestedTest.cpp)

# A cut below a unit subquery, which stops the walk that feeds it.
add_turing_test(test_query_ir_call_subquery_limit_below CallSubqueryLimitBelowTest.cpp)

# What a CALL before a read-only subquery body yielded is the query's result still.
add_call_v3_test(test_query_ir_call_subquery_yielded_output CallSubqueryYieldedOutputTest.cpp)

# A barrier of a subquery body ordered by an import the CALL carries into it.
add_turing_test(test_query_ir_call_subquery_import_order_by CallSubqueryImportOrderByTest.cpp)
target_include_directories(test_query_ir_call_subquery_import_order_by PRIVATE ${IR_TEST_DIR})

# OPTIONAL CALL: the columns the body was given, on the rows it yielded nothing for.
add_turing_test(test_query_ir_optional_subquery_carried_column OptionalSubqueryCarriedColumnTest.cpp)
target_include_directories(test_query_ir_optional_subquery_carried_column PRIVATE ${IR_TEST_DIR})

# UNION inside a CALL subquery body.
add_turing_test(test_query_ir_subquery_union_body SubqueryUnionBodyTest.cpp)

# UNION inside an EXISTS body.
add_turing_test(test_query_ir_exists_union_body ExistsUnionBodyTest.cpp)

# OPTIONAL CALL over a body yielding a nullable ID column.
add_ir_tests(test_query_ir_optional_subquery_nullable_yield OptionalSubqueryNullableYieldTest.cpp)
target_include_directories(test_query_ir_optional_subquery_nullable_yield PRIVATE ${IR_TEST_DIR})

# A constant a CALL subquery body returns beside a column of rows.
add_call_v3_test(test_query_ir_subquery_constant_return SubqueryConstantReturnTest.cpp)

# OPTIONAL CALL over a body returning a procedure string column.
add_call_v3_test(test_query_ir_optional_subquery_string_padding OptionalSubqueryStringPaddingTest.cpp)

# Which CALL subqueries may follow an updating clause.
add_turing_test(test_query_ir_subquery_after_update SubqueryAfterUpdateTest.cpp)
target_include_directories(test_query_ir_subquery_after_update PRIVATE ${IR_TEST_DIR})

# OPTIONAL CALL over a body returning a list.
add_call_v3_test(test_query_ir_optional_subquery_list_padding OptionalSubqueryListPaddingTest.cpp)

add_turing_test(test_query_ir_pattern_comprehension PatternComprehensionTest.cpp)
target_include_directories(test_query_ir_pattern_comprehension PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_pattern_comprehension_where PatternComprehensionWhereTest.cpp)
target_include_directories(test_query_ir_pattern_comprehension_where PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_subquery_constant_sort SubqueryConstantSortTest.cpp)
target_sources(test_query_ir_subquery_constant_sort PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_subquery_constant_sort PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_subquery_collect_sort SubqueryCollectSortTest.cpp)
target_sources(test_query_ir_subquery_collect_sort PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_subquery_collect_sort PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_subquery_body_with_beside_alias SubqueryBodyWithBesideAliasTest.cpp)

add_turing_test(test_query_ir_subquery_beside_expression_key SubqueryBesideExpressionKeyTest.cpp)

add_turing_test(test_query_ir_subquery_in_aggregate_argument SubqueryInAggregateArgumentTest.cpp)

add_turing_test(test_query_ir_subquery_ordered_on_alias SubqueryOrderedOnAliasTest.cpp)

add_turing_test(test_query_ir_subquery_body_holding_import SubqueryBodyHoldingImportTest.cpp)

add_turing_test(test_query_ir_subquery_body_naming_a_sibling_alias SubqueryBodyNamingASiblingAliasTest.cpp)

add_turing_test(test_query_ir_subquery_ordered_on_shadowing_alias SubqueryOrderedOnShadowingAliasTest.cpp)

add_turing_test(test_query_ir_subquery_with_beside_aggregate SubqueryWithBesideAggregateTest.cpp)

add_turing_test(test_query_ir_subquery_ordered_on_renamed_variable SubqueryOrderedOnRenamedVariableTest.cpp)

add_turing_test(test_query_ir_subquery_body_named_like_a_generated_column SubqueryBodyNamedLikeAGeneratedColumnTest.cpp)

add_turing_test(test_query_ir_subquery_ordered_after_grouping SubqueryOrderedAfterGroupingTest.cpp)

add_turing_test(test_query_ir_where_pattern_neo4j_manual WherePatternNeo4jManualTest.cpp)
target_include_directories(test_query_ir_where_pattern_neo4j_manual PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_pattern_predicate PatternPredicateTest.cpp)
target_include_directories(test_query_ir_pattern_predicate PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_exists_outer_edge ExistsOuterEdgeTest.cpp)
target_sources(test_query_ir_exists_outer_edge PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_exists_outer_edge PRIVATE ${IR_TEST_DIR})

# The imports of a CALL body read by the branches of its UNION.
add_turing_test(test_query_ir_call_subquery_union_import CallSubqueryUnionImportTest.cpp)

# An OPTIONAL CALL body that creates an entity and returns it for some of its input rows.
add_turing_test(test_query_ir_optional_call_written_entity OptionalCallWrittenEntityTest.cpp)

# A UNION in a CALL body returning an entity its branches created, merged or matched.
add_turing_test(test_query_ir_subquery_union_create_merge SubqueryUnionCreateMergeTest.cpp)

# An OPTIONAL CALL returning an entity its body created, padded where the body returns nothing.
add_turing_test(test_query_ir_optional_call_created_entity OptionalCallCreatedEntityTest.cpp)

# Every subquery form returning an entity a MATCH, MERGE or CREATE gave it, read by what follows.
add_turing_test(test_query_ir_subquery_written_entity_matrix SubqueryWrittenEntityMatrixTest.cpp)

# An EXISTS or COUNT subquery over a node the same query created.
add_turing_test(test_query_ir_exists_over_created_node ExistsOverCreatedNodeTest.cpp)

# A CALL () over rows holding only a projected constant leaves those rows in flight.
add_turing_test(test_query_ir_call_without_inputs_after_rows CallWithoutInputsAfterRowsTest.cpp)
