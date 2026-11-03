#include <gtest/gtest.h>

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "ParseOutcome.h"
#include "QueryMutants.h"

using namespace turing::test;

namespace {

struct ParseCase {
    std::string_view _query;
    std::string_view _outcome;
};

const ParseCase SCRIPTS[] = {
    {"",
     "       | Empty query\n"
     "     1 | \n"
     "       | \n"
     "-------* syntax error, unexpected PROG_END"},
    {";",
     "     1 | ;\n"
     "       | ^\n"
     "-------* syntax error, unexpected SEMI_COLON"},
    {"RETURN 1;",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`"},
    {"RETURN 1;;",
     "     1 | RETURN 1;;\n"
     "       |          ^\n"
     "-------* syntax error, unexpected SEMI_COLON"},
    {"RETURN 1; RETURN 2",
     "SINGLE_PART_QUERY\n"
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `2`"},
    {"  ",
     "     1 |   \n"
     "       | ^^\n"
     "-------* syntax error, unexpected PROG_END"},
    {"/* c */",
     "     1 | /* c */\n"
     "       | ^^^^^^^\n"
     "-------* syntax error, unexpected PROG_END"},
    {"// c",
     "     1 | // c\n"
     "       | ^^^^\n"
     "-------* syntax error, unexpected PROG_END"},
    {"RETURN 1 +",
     "     1 | RETURN 1 +\n"
     "       |          ^\n"
     "-------* syntax error, unexpected PROG_END"},
};

const ParseCase ADMINISTRATIVE_QUERIES[] = {
    {"CREATE GRAPH",
     "     1 | CREATE GRAPH\n"
     "       |        ^^^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting ASSIGN or a name"},
    {"CREATE GRAPH 99999999999999999999",
     "     1 | CREATE GRAPH 99999999999999999999\n"
     "       |              ^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Decimal literal exceeded int64 range."},
    {"CREATE GRAPH g",
     "CREATE_GRAPH_QUERY"},
    {"CREATE GRAPH = (a)",
     "SINGLE_PART_QUERY\n"
     "node `(a)`"},
    {"CREATE CONSTRAINT",
     "     1 | CREATE CONSTRAINT\n"
     "       |        ^^^^^^^^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting ASSIGN or a name"},
    {"CREATE CONSTRAINT null",
     "     1 | CREATE CONSTRAINT null\n"
     "       |                   ^^^^\n"
     "-------* syntax error, unexpected NULL_, expecting ASSIGN or a name"},
    {"CREATE CONSTRAINT c FOR (5",
     "     1 | CREATE CONSTRAINT c FOR (5\n"
     "       |                          ^\n"
     "-------* syntax error, unexpected DIGIT, expecting CPAREN or a name"},
    {"CREATE CONSTRAINT c FOR (",
     "     1 | CREATE CONSTRAINT c FOR (\n"
     "       |                         ^\n"
     "-------* syntax error, unexpected PROG_END, expecting CPAREN or a name"},
    {"CREATE CONSTRAINT c FOR (n:P) REQUIRE n.x IS UNIQUE",
     "     1 | CREATE CONSTRAINT c FOR (n:P) REQUIRE n.x IS UNIQUE\n"
     "       | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Not implemented: CREATE CONSTRAINT"},
    {"CREATE CONSTRAINT c IF NOT EXISTS FOR (n:P) REQUIRE n.x IS UNIQUE 5",
     "     1 | CREATE CONSTRAINT c IF NOT EXISTS FOR (n:P) REQUIRE n.x IS UNIQUE 5\n"
     "       | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Not implemented: CREATE CONSTRAINT"},
    {"CREATE CONSTRAINT c FOR ()-[r:T]-() REQUIRE r.x IS NOT NULL",
     "     1 | CREATE CONSTRAINT c FOR ()-[r:T]-() REQUIRE r.x IS NOT NULL\n"
     "       | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Not implemented: CREATE CONSTRAINT"},
    {"CREATE CONSTRAINT c FOR (n:P) REQUIRE n.x IS NOT 5",
     "     1 | CREATE CONSTRAINT c FOR (n:P) REQUIRE n.x IS NOT 5\n"
     "       |                                                  ^\n"
     "-------* syntax error, unexpected DIGIT, expecting NULL_"},
    {"CREATE VECTOR",
     "     1 | CREATE VECTOR\n"
     "       |        ^^^^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting ASSIGN or INDEX"},
    {"CREATE VECTOR 5",
     "     1 | CREATE VECTOR 5\n"
     "       |               ^\n"
     "-------* syntax error, unexpected DIGIT, expecting ASSIGN or INDEX"},
    {"CREATE VECTOR = (a)",
     "SINGLE_PART_QUERY\n"
     "node `(a)`"},
    {"CREATE VECTOR INDEX v WITH DIMENSION 4 METRIC EUCLID",
     "CREATE_VECTOR_INDEX_QUERY"},
    {"CREATE VECTOR INDEX v WITH DIMENSION 4 METRIC COSINE TYPE HNSW",
     "CREATE_VECTOR_INDEX_QUERY"},
    {"CREATE VECTOR INDEX v WITH DIMENSION 4 METRIC X",
     "     1 | CREATE VECTOR INDEX v WITH DIMENSION 4 METRIC X\n"
     "       |                                               ^\n"
     "-------* syntax error, unexpected ID, expecting EUCLID or COSINE"},
    {"CREATE INDEX",
     "     1 | CREATE INDEX\n"
     "       |        ^^^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting ASSIGN or a name"},
    {"CREATE INDEX i",
     "     1 | CREATE INDEX i\n"
     "       |              ^\n"
     "-------* syntax error, unexpected PROG_END, expecting FOR"},
    {"CREATE INDEX i 5",
     "     1 | CREATE INDEX i 5\n"
     "       |                ^\n"
     "-------* syntax error, unexpected DIGIT, expecting FOR"},
    {"CREATE INDEX i FOR",
     "     1 | CREATE INDEX i FOR\n"
     "       |                ^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting OPAREN or OBRACK"},
    {"CREATE INDEX i FOR 5",
     "     1 | CREATE INDEX i FOR 5\n"
     "       |                    ^\n"
     "-------* syntax error, unexpected DIGIT, expecting OPAREN or OBRACK"},
    {"CREATE INDEX i FOR (n:P) ON n.x",
     "CREATE_NODE_PROPERTY_INDEX_QUERY\n"
     "PROPERTY `n.x`\n"
     "node `(n:P)`"},
    {"CREATE INDEX i FOR (n:P) ON n.x 5",
     "     1 | CREATE INDEX i FOR (n:P) ON n.x 5\n"
     "       |                                 ^\n"
     "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON or DOT"},
    {"CREATE INDEX i FOR (n:P) ON (n).x 5",
     "     1 | CREATE INDEX i FOR (n:P) ON (n).x 5\n"
     "       |                                   ^\n"
     "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON"},
    {"CREATE INDEX i FOR (n WHERE n.x > 1) ON n.x",
     "     1 | CREATE INDEX i FOR (n WHERE n.x > 1) ON n.x\n"
     "       |                    ^^^^^^^^^^^^^^^^^\n"
     "-------* WHERE is not allowed in an index pattern"},
    {"CREATE INDEX i FOR (n WHERE n.x > 1) ON n.x 5",
     "     1 | CREATE INDEX i FOR (n WHERE n.x > 1) ON n.x 5\n"
     "       |                                             ^\n"
     "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON or DOT"},
    {"CREATE INDEX i FOR (n WHERE n.x > 1) ON (n).x 5",
     "     1 | CREATE INDEX i FOR (n WHERE n.x > 1) ON (n).x 5\n"
     "       |                    ^^^^^^^^^^^^^^^^^\n"
     "-------* WHERE is not allowed in an index pattern"},
    {"CREATE INDEX i FOR (n WHERE n.x > 1) ON (n).x;",
     "     1 | CREATE INDEX i FOR (n WHERE n.x > 1) ON (n).x;\n"
     "       |                    ^^^^^^^^^^^^^^^^^\n"
     "-------* WHERE is not allowed in an index pattern"},
    {"CREATE INDEX i FOR [e:T] ON e.x",
     "CREATE_EDGE_PROPERTY_INDEX_QUERY\n"
     "PROPERTY `e.x`\n"
     "edge `e:T`"},
    {"CREATE INDEX i FOR [] ON e.x",
     "CREATE_EDGE_PROPERTY_INDEX_QUERY\n"
     "PROPERTY `e.x`\n"
     "edge ``"},
    {"CREATE INDEX i FOR [e WHERE e.x > 1] ON e.x",
     "     1 | CREATE INDEX i FOR [e WHERE e.x > 1] ON e.x\n"
     "       |                     ^^^^^^^^^^^^^^^\n"
     "-------* WHERE is not allowed in an index pattern"},
    {"CREATE INDEX i FOR [e*1..2] ON e.x",
     "     1 | CREATE INDEX i FOR [e*1..2] ON e.x\n"
     "       |                     ^^^^^^\n"
     "-------* An edge property index takes no length quantifier"},
    {"CREATE INDEX i FOR [e*1..2 WHERE e.x > 1] ON e.x.y",
     "     1 | CREATE INDEX i FOR [e*1..2 WHERE e.x > 1] ON e.x.y\n"
     "       |                     ^^^^^^^^^^^^^^^^^^^^\n"
     "-------* WHERE is not allowed in an index pattern"},
    {"CREATE INDEX i FOR (n) ON (1).x",
     "     1 | CREATE INDEX i FOR (n) ON (1).x\n"
     "       |                           ^^^\n"
     "-------* Invalid property access. Only a variable can be parenthesized on the left of '.'"},
    {"CREATE INDEX i FOR (n) ON (n.y).x",
     "     1 | CREATE INDEX i FOR (n) ON (n.y).x\n"
     "       |                           ^^^^^\n"
     "-------* Invalid property access. Only a variable can be parenthesized on the left of '.'"},
    {"CREATE INDEX i FOR (n) ON 99999999999999999999",
     "     1 | CREATE INDEX i FOR (n) ON 99999999999999999999\n"
     "       |                           ^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Decimal literal exceeded int64 range."},
    {"DROP",
     "     1 | DROP\n"
     "       | ^^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting CONSTRAINT or INDEX"},
    {"DROP 5",
     "     1 | DROP 5\n"
     "       |      ^\n"
     "-------* syntax error, unexpected DIGIT, expecting CONSTRAINT or INDEX"},
    {"DROP INDEX",
     "     1 | DROP INDEX\n"
     "       |      ^^^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting a name"},
    {"DROP INDEX i",
     "DROP_INDEX_QUERY"},
    {"DROP CONSTRAINT foo",
     "     1 | DROP CONSTRAINT foo\n"
     "       | ^^^^^^^^^^^^^^^^^^^\n"
     "-------* Not implemented: DROP CONSTRAINT"},
    {"DROP CONSTRAINT foo 5",
     "     1 | DROP CONSTRAINT foo 5\n"
     "       |                     ^\n"
     "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON or IF"},
    {"DROP CONSTRAINT foo;",
     "     1 | DROP CONSTRAINT foo;\n"
     "       | ^^^^^^^^^^^^^^^^^^^\n"
     "-------* Not implemented: DROP CONSTRAINT"},
    {"DROP CONSTRAINT foo IF EXISTS",
     "     1 | DROP CONSTRAINT foo IF EXISTS\n"
     "       | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Not implemented: DROP CONSTRAINT"},
    {"DROP CONSTRAINT foo IF EXISTS 5",
     "     1 | DROP CONSTRAINT foo IF EXISTS 5\n"
     "       | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Not implemented: DROP CONSTRAINT"},
    {"DROP CONSTRAINT foo IF 5",
     "     1 | DROP CONSTRAINT foo IF 5\n"
     "       |                        ^\n"
     "-------* syntax error, unexpected DIGIT, expecting EXISTS"},
    {"LOAD",
     "     1 | LOAD\n"
     "       | ^^^^\n"
     "-------* syntax error, unexpected PROG_END"},
    {"LOAD 5",
     "     1 | LOAD 5\n"
     "       |      ^\n"
     "-------* syntax error, unexpected DIGIT"},
    {"LOAD GRAPH",
     "     1 | LOAD GRAPH\n"
     "       |      ^^^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting a name"},
    {"LOAD GRAPH g",
     "LOAD_GRAPH_QUERY"},
    {"LOAD JSONL 'f'",
     "LOAD_JSONL_QUERY"},
    {"LOAD JSONL 'f' AS g",
     "LOAD_JSONL_QUERY"},
    {"LOAD JSONL 'f' WITH EMBEDDINGS [{e, 4}] WITH DATETIMES ['d', t] WITH DURATIONS [u]",
     "LOAD_JSONL_QUERY"},
    {"LOAD JSONL 'f' WITH 5",
     "     1 | LOAD JSONL 'f' WITH 5\n"
     "       |                     ^\n"
     "-------* syntax error, unexpected DIGIT, expecting DATETIMES or DURATIONS or EMBEDDINGS"},
    {"LOAD GML 'f'",
     "LOAD_GML_QUERY"},
    {"LOAD GML 'f' AS g",
     "LOAD_GML_QUERY"},
    {"LOAD PARQUET 'f' AS g WITH DURATIONS [d, 'e']",
     "LOAD_PARQUET_QUERY"},
    {"LOAD PARQUET 'f' 5",
     "     1 | LOAD PARQUET 'f' 5\n"
     "       |                  ^\n"
     "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON or WITH or AS"},
    {"LOAD COMMIT 'h'",
     "LOAD_COMMIT_QUERY"},
    {"LOAD VECTOR FROM 'f' IN v",
     "LOAD_VECTOR_QUERY"},
    {"LOAD EMBEDDING FROM 'f' AS e",
     "LOAD_EMBEDDING_QUERY"},
    {"LIST",
     "     1 | LIST\n"
     "       | ^^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting GRAPH or AVAILABLE"},
    {"LIST GRAPH",
     "LIST_GRAPH_QUERY"},
    {"LIST AVAILABLE GRAPHS",
     "LIST_AVAILABLE_GRAPHS_QUERY"},
    {"LIST 5",
     "     1 | LIST 5\n"
     "       |      ^\n"
     "-------* syntax error, unexpected DIGIT, expecting GRAPH or AVAILABLE"},
    {"SHOW",
     "     1 | SHOW\n"
     "       | ^^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting PROCEDURES or EXTENSIONS or VECTOR"},
    {"SHOW PROCEDURES",
     "SHOW_PROCEDURES_QUERY"},
    {"SHOW EXTENSIONS",
     "SHOW_EXTENSIONS_QUERY"},
    {"SHOW VECTOR INDEXES",
     "SHOW_VECTOR_INDEXES_QUERY"},
    {"SHOW 5",
     "     1 | SHOW 5\n"
     "       |      ^\n"
     "-------* syntax error, unexpected DIGIT, expecting PROCEDURES or EXTENSIONS or VECTOR"},
    {"S3",
     "     1 | S3\n"
     "       | ^^\n"
     "-------* syntax error, unexpected PROG_END, expecting CONNECT or PUSH or PULL"},
    {"S3 CONNECT 'a' 'b' 'c'",
     "S3_CONNECT_QUERY"},
    {"S3 PULL 'a' 'b'",
     "S3_TRANSFER_QUERY"},
    {"S3 PUSH 'a' 'b'",
     "S3_TRANSFER_QUERY"},
    {"S3 5",
     "     1 | S3 5\n"
     "       |    ^\n"
     "-------* syntax error, unexpected DIGIT, expecting CONNECT or PUSH or PULL"},
    {"DELETE VECTOR INDEX v",
     "DELETE_VECTOR_INDEX_QUERY"},
    {"DELETE VECTOR",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `VECTOR`"},
    {"DELETE VECTOR 5",
     "     1 | DELETE VECTOR 5\n"
     "       |               ^\n"
     "-------* syntax error, unexpected DIGIT"},
    {"CHANGE NEW",
     "CHANGE_QUERY"},
    {"CHANGE SUBMIT",
     "CHANGE_QUERY"},
    {"CHANGE DELETE",
     "CHANGE_QUERY"},
    {"CHANGE LIST",
     "CHANGE_QUERY"},
    {"CHANGE 5",
     "     1 | CHANGE 5\n"
     "       |        ^\n"
     "-------* syntax error, unexpected DIGIT, expecting SUBMIT or DELETE or LIST or NEW"},
    {"COMMIT",
     "COMMIT_QUERY"},
    {"COMMIT 5",
     "     1 | COMMIT 5\n"
     "       |        ^\n"
     "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON"},
    {"MERGE_DATAPARTS",
     "MERGE_DATAPARTS_QUERY"},
    {"INSTALL x",
     "INSTALL_EXTENSION_QUERY"},
    {"INSTALL",
     "     1 | INSTALL\n"
     "       | ^^^^^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting a name"},
};

const ParseCase EXPLAIN_OPTIONS[] = {
    {"EXPLAIN RETURN 1",
     "SINGLE_PART_QUERY\n"
     "explain db nl\n"
     "LITERAL `1`"},
    {"EXPLAIN (ast, nl) RETURN 1",
     "SINGLE_PART_QUERY\n"
     "explain ast nl\n"
     "LITERAL `1`"},
    {"EXPLAIN (all) RETURN 1",
     "SINGLE_PART_QUERY\n"
     "explain ast vdg codegen passes db nl\n"
     "LITERAL `1`"},
    {"EXPLAIN (bogus) RETURN 1",
     "     1 | EXPLAIN (bogus) RETURN 1\n"
     "       |          ^^^^^\n"
     "-------* Unknown EXPLAIN option 'bogus'. Expected one of ast, vdg, codegen, passes, db, nl, all, or before, after, around followed by a pass name"},
    {"EXPLAIN (bogus 5",
     "     1 | EXPLAIN (bogus 5\n"
     "       |          ^^^^^\n"
     "-------* Unknown EXPLAIN option 'bogus'. Expected one of ast, vdg, codegen, passes, db, nl, all, or before, after, around followed by a pass name"},
    {"EXPLAIN (ast 5",
     "     1 | EXPLAIN (ast 5\n"
     "       |              ^\n"
     "-------* syntax error, unexpected DIGIT, expecting CPAREN or OBRACK or COMMA or a name"},
    {"EXPLAIN (before x) RETURN 1",
     "SINGLE_PART_QUERY\n"
     "explain with named passes\n"
     "LITERAL `1`"},
    {"EXPLAIN (before [x, y]) RETURN 1",
     "SINGLE_PART_QUERY\n"
     "explain with named passes\n"
     "LITERAL `1`"},
    {"EXPLAIN (bogus x) RETURN 1",
     "     1 | EXPLAIN (bogus x) RETURN 1\n"
     "       |          ^^^^^\n"
     "-------* Unknown EXPLAIN option 'bogus'. Expected one of ast, vdg, codegen, passes, db, nl, all, or before, after, around followed by a pass name"},
    {"EXPLAIN (ast x) RETURN 1",
     "     1 | EXPLAIN (ast x) RETURN 1\n"
     "       |          ^^^\n"
     "-------* Unknown EXPLAIN option 'ast'. Expected one of ast, vdg, codegen, passes, db, nl, all, or before, after, around followed by a pass name"},
    {"EXPLAIN (before [x 5",
     "     1 | EXPLAIN (before [x 5\n"
     "       |                    ^\n"
     "-------* syntax error, unexpected DIGIT, expecting CBRACK or COMMA"},
    {"EXPLAIN (ast) LOAD GRAPH g",
     "LOAD_GRAPH_QUERY\n"
     "explain ast"},
    {"EXPLAIN LOAD GRAPH g 5",
     "     1 | EXPLAIN LOAD GRAPH g 5\n"
     "       |                      ^\n"
     "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON"},
    {"EXPLAIN 5",
     "     1 | EXPLAIN 5\n"
     "       |         ^\n"
     "-------* syntax error, unexpected DIGIT"},
};

const ParseCase QUERY_COMPOSITIONS[] = {
    {"RETURN EXISTS { { MATCH (a) RETURN a } UNION { MATCH (b) RETURN b } }",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "SYMBOL `b`\n"
     "EXISTS `EXISTS { { MATCH (a) RETURN a } UNION { MATCH (b) RETURN b } }`\n"
     "node `(a)`\n"
     "node `(b)`"},
    {"WITH 1 AS x RETURN x NEXT RETURN x + 1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1 AS x`\n"
     "SYMBOL `x`\n"
     "SYMBOL `x`\n"
     "LITERAL `1`\n"
     "BINARY `x + 1`"},
    {"WITH 1 AS x RETURN x + 1 NEXT RETURN 2",
     "     1 | WITH 1 AS x RETURN x + 1 NEXT RETURN 2\n"
     "       | ^^^^^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Expression in a RETURN followed by NEXT must be aliased (use AS)"},
    {"RETURN 1 AS a NEXT RETURN a NEXT RETURN a",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1 AS a`\n"
     "SYMBOL `a`\n"
     "SYMBOL `a`"},
    {"RETURN 1 NEXT",
     "     1 | RETURN 1 NEXT\n"
     "       | ^^^^^^^^\n"
     "-------* Expression in a RETURN followed by NEXT must be aliased (use AS)"},
    {"RETURN 1 AS a UNION RETURN 2 AS a NEXT RETURN a",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1 AS a`\n"
     "LITERAL `2 AS a`\n"
     "SYMBOL `a`"},
    {"{ RETURN 1 AS a } UNION { RETURN 2 AS a }",
     "UNION_QUERY\n"
     "LITERAL `1 AS a`\n"
     "LITERAL `2 AS a`"},
    {"{ RETURN 1 }",
     "     1 | { RETURN 1 }\n"
     "       |            ^\n"
     "-------* syntax error, unexpected PROG_END, expecting UNION"},
    {"{ RETURN 1 } 5",
     "     1 | { RETURN 1 } 5\n"
     "       |              ^\n"
     "-------* syntax error, unexpected DIGIT, expecting UNION"},
    {"WHEN true THEN RETURN 1 ELSE RETURN 2",
     "CONDITIONAL_QUERY\n"
     "LITERAL `true`\n"
     "LITERAL `1`\n"
     "LITERAL `2`"},
    {"WHEN true THEN RETURN 1 AS a NEXT RETURN a",
     "SINGLE_PART_QUERY\n"
     "LITERAL `true`\n"
     "LITERAL `1 AS a`\n"
     "SYMBOL `a`"},
    {"WHEN true THEN { RETURN 1 } WHEN false THEN RETURN 2",
     "CONDITIONAL_QUERY\n"
     "LITERAL `true`\n"
     "LITERAL `1`\n"
     "LITERAL `false`\n"
     "LITERAL `2`"},
    {"RETURN 1 UNION ALL RETURN 2",
     "UNION_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `2`"},
    {"RETURN 1 UNION 5",
     "     1 | RETURN 1 UNION 5\n"
     "       |                ^\n"
     "-------* syntax error, unexpected DIGIT"},
};

const ParseCase CALLS_AND_SUBQUERIES[] = {
    {"OPTIONAL 5",
     "     1 | OPTIONAL 5\n"
     "       |          ^\n"
     "-------* syntax error, unexpected DIGIT, expecting MATCH or CALL"},
    {"OPTIONAL MATCH (n) RETURN n",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `n`\n"
     "node `(n)`"},
    {"OPTIONAL CALL 5",
     "     1 | OPTIONAL CALL 5\n"
     "       |               ^\n"
     "-------* syntax error, unexpected DIGIT, expecting OPAREN or OBRACE or a name"},
    {"OPTIONAL CALL f() 5",
     "     1 | OPTIONAL CALL f() 5\n"
     "       |                   ^\n"
     "-------* syntax error, unexpected DIGIT"},
    {"CALL 5",
     "     1 | CALL 5\n"
     "       |      ^\n"
     "-------* syntax error, unexpected DIGIT, expecting OPAREN or OBRACE or a name"},
    {"CALL f()",
     "SINGLE_PART_QUERY\n"
     "FUNCTION_INVOCATION (no location)"},
    {"CALL f() YIELD *",
     "SINGLE_PART_QUERY\n"
     "FUNCTION_INVOCATION (no location)"},
    {"CALL f() YIELD a AS b, c WHERE c > 1 RETURN b",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a AS b`\n"
     "SYMBOL `c`\n"
     "SYMBOL `c`\n"
     "LITERAL `1`\n"
     "BINARY `c > 1`\n"
     "FUNCTION_INVOCATION (no location)\n"
     "SYMBOL `b`"},
    {"CALL db.labels",
     "     1 | CALL db.labels\n"
     "       |         ^^^^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting OPAREN or DOT"},
    {"CALL { RETURN 1 }",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`"},
    {"CALL () { RETURN 1 }",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`"},
    {"CALL (*) { RETURN 1 }",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`"},
    {"CALL (a, b) { RETURN 1 }",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`"},
    {"CALL (5",
     "     1 | CALL (5\n"
     "       |       ^\n"
     "-------* syntax error, unexpected DIGIT, expecting CPAREN or MULT or a name"},
    {"MATCH (n) CALL (n) { RETURN 1 } RETURN n",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "SYMBOL `n`\n"
     "node `(n)`"},
    {"OPTIONAL CALL (n) { RETURN 1 }",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`"},
    {"CALL 99999999999999999999",
     "     1 | CALL 99999999999999999999\n"
     "       |      ^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Decimal literal exceeded int64 range."},
    {"RETURN EXISTS { }",
     "     1 | RETURN EXISTS { }\n"
     "       |                 ^\n"
     "-------* syntax error, unexpected CBRACE, expecting OPAREN or OBRACE or a name"},
    {"RETURN EXISTS { 1 }",
     "     1 | RETURN EXISTS { 1 }\n"
     "       |                 ^\n"
     "-------* syntax error, unexpected DIGIT, expecting OPAREN or OBRACE or a name"},
    {"RETURN COUNT { $x }",
     "     1 | RETURN COUNT { $x }\n"
     "       |                ^\n"
     "-------* syntax error, unexpected DOLLAR, expecting OPAREN or OBRACE or a name"},
    {"RETURN EXISTS { MATCH }",
     "     1 | RETURN EXISTS { MATCH }\n"
     "       |                       ^\n"
     "-------* syntax error, unexpected CBRACE, expecting OPAREN or ASSIGN or a name"},
    {"RETURN EXISTS { OPTIONAL 5 }",
     "     1 | RETURN EXISTS { OPTIONAL 5 }\n"
     "       |                          ^\n"
     "-------* syntax error, unexpected DIGIT, expecting ASSIGN or MATCH or CALL"},
    {"RETURN EXISTS { CALL 5 }",
     "     1 | RETURN EXISTS { CALL 5 }\n"
     "       |                      ^\n"
     "-------* syntax error, unexpected DIGIT, expecting OPAREN or OBRACE or ASSIGN or a name"},
    {"RETURN EXISTS { foo }",
     "     1 | RETURN EXISTS { foo }\n"
     "       |                     ^\n"
     "-------* syntax error, unexpected CBRACE, expecting ASSIGN"},
    {"RETURN EXISTS { foo = (a) }",
     "SINGLE_PART_QUERY\n"
     "EXISTS `EXISTS { foo = (a) }`\n"
     "node `(a)`"},
    {"RETURN EXISTS { match = (a) }",
     "SINGLE_PART_QUERY\n"
     "EXISTS `EXISTS { match = (a) }`\n"
     "node `(a)`"},
    {"RETURN EXISTS { (a)-->(b) WHERE a.x > 1 }",
     "SINGLE_PART_QUERY\n"
     "PROPERTY `a.x`\n"
     "LITERAL `1`\n"
     "BINARY `a.x > 1`\n"
     "EXISTS `EXISTS { (a)-->(b) WHERE a.x > 1 }`\n"
     "node `(a)`\n"
     "edge `-->`\n"
     "node `(b)`"},
    {"RETURN EXISTS { MATCH (a) RETURN a }",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "EXISTS `EXISTS { MATCH (a) RETURN a }`\n"
     "node `(a)`"},
    {"RETURN EXISTS { WHEN true THEN RETURN 1 }",
     "SINGLE_PART_QUERY\n"
     "LITERAL `true`\n"
     "LITERAL `1`\n"
     "EXISTS `EXISTS { WHEN true THEN RETURN 1 }`"},
    {"RETURN EXISTS { 99999999999999999999 }",
     "     1 | RETURN EXISTS { 99999999999999999999 }\n"
     "       |                 ^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Decimal literal exceeded int64 range."},
    {"RETURN EXISTS { MATCH 99999999999999999999 }",
     "     1 | RETURN EXISTS { MATCH 99999999999999999999 }\n"
     "       |                       ^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Decimal literal exceeded int64 range."},
    {"RETURN EXISTS(n.x)",
     "     1 | RETURN EXISTS(n.x)\n"
     "       |               ^^^\n"
     "-------* exists() takes a pattern: test a property with IS NOT NULL"},
    {"RETURN EXISTS((a)-->(b))",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "EXISTS `EXISTS((a)-->(b))`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "node `(a)-->(b)`"},
    {"RETURN COUNT { (a) }",
     "SINGLE_PART_QUERY\n"
     "COUNT_SUBQUERY `COUNT { (a) }`\n"
     "node `(a)`"},
    {"RETURN COLLECT { RETURN 1 }",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "COLLECT_SUBQUERY `COLLECT { RETURN 1 }`"},
    {"MATCH (n) OPTIONAL 5",
     "     1 | MATCH (n) OPTIONAL 5\n"
     "       |                    ^\n"
     "-------* syntax error, unexpected DIGIT, expecting MATCH or CALL"},
};

const ParseCase VALUE_TESTS_AND_CASES[] = {
    {"RETURN 1 IS NOT 5",
     "     1 | RETURN 1 IS NOT 5\n"
     "       |             ^^^^^\n"
     "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED"},
    {"RETURN 'a' IS NOT NFC 5",
     "     1 | RETURN 'a' IS NOT NFC 5\n"
     "       |               ^^^^^^^^^\n"
     "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED"},
    {"RETURN 1 IS NOT (NULL)",
     "     1 | RETURN 1 IS NOT (NULL)\n"
     "       |             ^^^^^\n"
     "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED"},
    {"RETURN 1 IS NFC 5",
     "     1 | RETURN 1 IS NFC 5\n"
     "       |             ^^^^^\n"
     "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED"},
    {"RETURN 1 IS NULL",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `NULL`\n"
     "BINARY `1 IS NULL`"},
    {"RETURN 1 IS NOT NULL",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `NOT NULL`\n"
     "BINARY `1 IS NOT NULL`"},
    {"RETURN 'a' IS NORMALIZED",
     "SINGLE_PART_QUERY\n"
     "LITERAL `'a'`\n"
     "LITERAL (no location)\n"
     "FUNCTION_INVOCATION `'a' IS NORMALIZED`"},
    {"RETURN 'a' IS NOT NFKD NORMALIZED",
     "SINGLE_PART_QUERY\n"
     "LITERAL `'a'`\n"
     "LITERAL (no location)\n"
     "FUNCTION_INVOCATION `'a' IS NOT NFKD NORMALIZED`\n"
     "UNARY `'a' IS NOT NFKD NORMALIZED`"},
    {"RETURN 1 IS NOT 99999999999999999999",
     "     1 | RETURN 1 IS NOT 99999999999999999999\n"
     "       |                 ^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Decimal literal exceeded int64 range."},
    {"RETURN 1 IS NFC 99999999999999999999",
     "     1 | RETURN 1 IS NFC 99999999999999999999\n"
     "       |                 ^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Decimal literal exceeded int64 range."},
    {"RETURN 1 IS NFC",
     "     1 | RETURN 1 IS NFC\n"
     "       |             ^^^\n"
     "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED"},
    {"RETURN 1 IS NOT",
     "     1 | RETURN 1 IS NOT\n"
     "       |             ^^^\n"
     "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED"},
    {"RETURN 1 IS NOT NULL IS NULL",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `NOT NULL`\n"
     "BINARY `1 IS NOT NULL`\n"
     "LITERAL `NULL`\n"
     "BINARY `1 IS NOT NULL IS NULL`"},
    {"RETURN 1 < 2 <= 3 IS NULL = true",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `2`\n"
     "BINARY `1 < 2`\n"
     "LITERAL `3`\n"
     "BINARY `2 <= 3`\n"
     "LOGICAL `1 < 2 <= 3`\n"
     "LITERAL `NULL`\n"
     "BINARY `1 < 2 <= 3 IS NULL`\n"
     "LITERAL `true`\n"
     "BINARY `1 < 2 <= 3 IS NULL = true`"},
    {"RETURN CASE WHEN IS NOT 5 THEN 1 END",
     "     1 | RETURN CASE WHEN IS NOT 5 THEN 1 END\n"
     "       |                         ^\n"
     "-------* syntax error, unexpected DIGIT, expecting NULL_"},
    {"RETURN CASE 1 WHEN IS NULL THEN 2 END",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `2`\n"
     "CASE `CASE 1 WHEN IS NULL THEN 2 END`"},
    {"RETURN CASE 1 WHEN < 2, = 3 THEN 4 ELSE 5 END",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `2`\n"
     "LITERAL `3`\n"
     "LITERAL `4`\n"
     "CASE `CASE 1 WHEN < 2, = 3 THEN 4 ELSE 5 END`\n"
     "LITERAL `5`"},
    {"RETURN CASE WHEN x IS NOT 5 THEN 1 END",
     "     1 | RETURN CASE WHEN x IS NOT 5 THEN 1 END\n"
     "       |                       ^^^^^\n"
     "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED"},
};

const ParseCase UPDATING_CLAUSES[] = {
    {"MATCH (vector) DELETE vector",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `vector`\n"
     "node `(vector)`"},
    {"MATCH (n) REMOVE n:A",
     "     1 | MATCH (n) REMOVE n:A\n"
     "       |                  ^^^\n"
     "-------* Not implemented: REMOVE of a label"},
    {"MATCH (n) REMOVE n:A 5",
     "     1 | MATCH (n) REMOVE n:A 5\n"
     "       |                      ^\n"
     "-------* syntax error, unexpected DIGIT"},
    {"MATCH (n) REMOVE n:A, n.x",
     "     1 | MATCH (n) REMOVE n:A, n.x\n"
     "       |                  ^^^\n"
     "-------* Not implemented: REMOVE of a label"},
    {"MATCH (n) REMOVE n IS A",
     "     1 | MATCH (n) REMOVE n IS A\n"
     "       |                  ^^^^^^\n"
     "-------* Not implemented: REMOVE of a label"},
    {"MATCH (n) REMOVE (n).x",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `n`\n"
     "PROPERTY `(n).x`\n"
     "node `(n)`"},
    {"MATCH (n) REMOVE (1).x",
     "     1 | MATCH (n) REMOVE (1).x\n"
     "       |                  ^^^\n"
     "-------* Invalid property access. Only a variable can be parenthesized on the left of '.'"},
    {"MATCH (n) REMOVE n 5",
     "     1 | MATCH (n) REMOVE n 5\n"
     "       |                    ^\n"
     "-------* syntax error, unexpected DIGIT, expecting COLON or DOT or IS"},
    {"MATCH (n) SET n = 1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "SYMBOL `n`\n"
     "node `(n)`"},
    {"MATCH (n) SET n 5",
     "     1 | MATCH (n) SET n 5\n"
     "       |                 ^\n"
     "-------* syntax error, unexpected DIGIT"},
    {"MATCH (n) SET n += {a: 1}",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `{a: 1}`\n"
     "SYMBOL `n`\n"
     "node `(n)`"},
    {"MATCH (n) SET n:A:B",
     "SINGLE_PART_QUERY\n"
     "ENTITY_TYPES `n:A:B`\n"
     "node `(n)`"},
    {"MATCH (n) SET (n).x = 1, n.y.z = 2",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `n`\n"
     "PROPERTY `(n).x`\n"
     "LITERAL `1`\n"
     "PROPERTY `n.y.z`\n"
     "LITERAL `2`\n"
     "node `(n)`"},
    {"MATCH (n) SET (1).x = 1",
     "     1 | MATCH (n) SET (1).x = 1\n"
     "       |               ^^^\n"
     "-------* Invalid property access. Only a variable can be parenthesized on the left of '.'"},
    {"FOREACH (x IN n:A|B | SET x.y = 1)",
     "     1 | FOREACH (x IN n:A|B | SET x.y = 1)\n"
     "       |                   ^\n"
     "-------* syntax error, unexpected ID"},
    {"MERGE (a) ON CREATE SET a.x = 1 ON MATCH SET a.y = 2 ON CREATE SET a.z = 3",
     "SINGLE_PART_QUERY\n"
     "PROPERTY `a.x`\n"
     "LITERAL `1`\n"
     "PROPERTY `a.y`\n"
     "LITERAL `2`\n"
     "PROPERTY `a.z`\n"
     "LITERAL `3`\n"
     "node `(a)`"},
    {"MERGE (a) ON 5",
     "     1 | MERGE (a) ON 5\n"
     "       |              ^\n"
     "-------* syntax error, unexpected DIGIT, expecting CREATE or MATCH"},
    {"MERGE p = (a)",
     "SINGLE_PART_QUERY\n"
     "node `(a)`"},
    {"FOREACH (x IN [1] | CREATE (a) SET a.x = 1 MERGE (b) DELETE b REMOVE a.x FOREACH (y IN [2] | CREATE (c)))",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `[1]`\n"
     "PROPERTY `a.x`\n"
     "LITERAL `1`\n"
     "SYMBOL `b`\n"
     "PROPERTY `a.x`\n"
     "LITERAL `2`\n"
     "LITERAL `[2]`\n"
     "node `(a)`\n"
     "node `(b)`\n"
     "node `(c)`"},
    {"FOREACH (x IN [1] | RETURN 1)",
     "     1 | FOREACH (x IN [1] | RETURN 1)\n"
     "       |                     ^^^^^^\n"
     "-------* syntax error, unexpected RETURN"},
    {"MATCH (n) DETACH DELETE n, m",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `n`\n"
     "SYMBOL `m`\n"
     "node `(n)`"},
    {"MATCH (n) DETACH 5",
     "     1 | MATCH (n) DETACH 5\n"
     "       |                  ^\n"
     "-------* syntax error, unexpected DIGIT, expecting DELETE"},
    {"CREATE (a)-[:T {x: 1}]->(b), (c)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "node `(a)`\n"
     "edge `-[:T {x: 1}]->`\n"
     "node `(b)`\n"
     "node `(c)`"},
};

const ParseCase RELATIONSHIPS[] = {
    {"MATCH (a)-->(b) RETURN 1 - [1, 2][0]",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `1`\n"
     "LITERAL `2`\n"
     "LITERAL `[1, 2]`\n"
     "LITERAL `0`\n"
     "INDEX `[1, 2][0]`\n"
     "BINARY `1 - [1, 2][0]`\n"
     "node `(a)`\n"
     "edge `-->`\n"
     "node `(b)`"},
    {"RETURN 1 - [1, 2]",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `1`\n"
     "LITERAL `2`\n"
     "LITERAL `[1, 2]`\n"
     "BINARY `1 - [1, 2]`"},
    {"RETURN -[1, 2][0]",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `2`\n"
     "LITERAL `[1, 2]`\n"
     "LITERAL `0`\n"
     "INDEX `[1, 2][0]`\n"
     "UNARY `-[1, 2][0]`"},
    {"WITH 1 AS x WHERE x <--1 RETURN x",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1 AS x`\n"
     "SYMBOL `x`\n"
     "LITERAL `1`\n"
     "LITERAL `-1`\n"
     "LITERAL `--1`\n"
     "BINARY `x <--1`\n"
     "SYMBOL `x`"},
    {"WITH 1 AS x WHERE (x) < - -2 RETURN x",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1 AS x`\n"
     "SYMBOL `x`\n"
     "LITERAL `2`\n"
     "LITERAL `-2`\n"
     "LITERAL `- -2`\n"
     "BINARY `(x) < - -2`\n"
     "SYMBOL `x`"},
    {"RETURN (1) - [1, 2][1]",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `1`\n"
     "LITERAL `2`\n"
     "LITERAL `[1, 2]`\n"
     "LITERAL `1`\n"
     "INDEX `[1, 2][1]`\n"
     "BINARY `(1) - [1, 2][1]`"},
    {"MATCH (a {x: -[1][0]}) RETURN a",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `[1]`\n"
     "LITERAL `0`\n"
     "INDEX `[1][0]`\n"
     "UNARY `-[1][0]`\n"
     "SYMBOL `a`\n"
     "node `(a {x: -[1][0]})`"},
    {"MATCH ()-[e WHERE e.d > -[1][0]]->() RETURN e",
     "SINGLE_PART_QUERY\n"
     "PROPERTY `e.d`\n"
     "LITERAL `1`\n"
     "LITERAL `[1]`\n"
     "LITERAL `0`\n"
     "INDEX `[1][0]`\n"
     "UNARY `-[1][0]`\n"
     "BINARY `e.d > -[1][0]`\n"
     "SYMBOL `e`\n"
     "node `()`\n"
     "edge `-[e WHERE e.d > -[1][0]]->`\n"
     "node `()`"},
    {"MATCH (a)-\n->(b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-\\n->`\n"
     "node `(b)`"},
    {"MATCH (a) < - [e] - (b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `< - [e] -`\n"
     "node `(b)`"},
    {"MATCH (a)-(b) RETURN a",
     "     1 | MATCH (a)-(b) RETURN a\n"
     "       |           ^\n"
     "-------* syntax error, unexpected OPAREN, expecting OBRACK or SUB"},
    {"MATCH (a)<(b) RETURN a",
     "     1 | MATCH (a)<(b) RETURN a\n"
     "       |           ^\n"
     "-------* syntax error, unexpected OPAREN, expecting SUB"},
    {"MATCH (a)<-- RETURN a",
     "     1 | MATCH (a)<-- RETURN a\n"
     "       |              ^^^^^^\n"
     "-------* syntax error, unexpected RETURN"},
    {"MATCH (a)--> RETURN a",
     "     1 | MATCH (a)--> RETURN a\n"
     "       |              ^^^^^^\n"
     "-------* syntax error, unexpected RETURN, expecting OPAREN or OBRACE or PLUS or MULT"},
    {"MATCH (a)-",
     "     1 | MATCH (a)-\n"
     "       |          ^\n"
     "-------* syntax error, unexpected PROG_END, expecting OBRACK or SUB"},
    {"MATCH (a)-[e]-",
     "     1 | MATCH (a)-[e]-\n"
     "       |              ^\n"
     "-------* syntax error, unexpected PROG_END"},
    {"MATCH (a)-[e]->(b)<-[f]-(c)--(d)<-->(e)-[]-(f) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-[e]->`\n"
     "node `(b)`\n"
     "edge `<-[f]-`\n"
     "node `(c)`\n"
     "edge `--`\n"
     "node `(d)`\n"
     "edge `<-->`\n"
     "node `(e)`\n"
     "edge `-[]-`\n"
     "node `(f)`"},
    {"MATCH (a)-[*]->(b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-[*]->`\n"
     "node `(b)`\n"
     "edge `-[*]->`"},
    {"MATCH (a)-[*2]->(b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-[*2]->`\n"
     "node `(b)`\n"
     "edge `-[*2]->`"},
    {"MATCH (a)-[*2..]->(b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-[*2..]->`\n"
     "node `(b)`\n"
     "edge `-[*2..]->`"},
    {"MATCH (a)-[*..3]->(b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-[*..3]->`\n"
     "node `(b)`\n"
     "edge `-[*..3]->`"},
    {"MATCH (a)-[*..]->(b) RETURN a",
     "     1 | MATCH (a)-[*..]->(b) RETURN a\n"
     "       |               ^\n"
     "-------* syntax error, unexpected CBRACK, expecting DIGIT"},
    {"MATCH (a)-[e*1..2]->{1,3}(b) RETURN a",
     "     1 | MATCH (a)-[e*1..2]->{1,3}(b) RETURN a\n"
     "       |                     ^^^^^\n"
     "-------* A relationship takes one length quantifier: either [e*1..3] inside the brackets or {1,3} after them"},
    {"MATCH (a)-->{1,3}(b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "edge `-->`"},
    {"MATCH (a)-->+(b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "edge `-->`"},
    {"MATCH (a)-->*(b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "edge `-->`"},
    {"MATCH (a)-->{,}(b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "edge `-->`"},
    {"MATCH (a)-->{2}(b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "edge `-->`"},
    {"MATCH (a)-->{2,}(b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "edge `-->`"},
    {"MATCH (a)-->{,2}(b) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "edge `-->`"},
    {"MATCH (a)-->{x}(b) RETURN a",
     "     1 | MATCH (a)-->{x}(b) RETURN a\n"
     "       |              ^\n"
     "-------* syntax error, unexpected ID, expecting COMMA or DIGIT"},
    {"MATCH (a)((b)-->(c))+(d) RETURN a",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "node `(b)`\n"
     "edge `-->`\n"
     "node `(c)`\n"
     "node `(d)`\n"
     "edge `(b)-->(c)`"},
    {"MATCH (a)((b))+(d) RETURN a",
     "     1 | MATCH (a)((b))+(d) RETURN a\n"
     "       |           ^^^\n"
     "-------* A quantified pattern repeats at least one relationship"},
    {"MATCH (a)((b)-[*2]->(c))+(d) RETURN a",
     "     1 | MATCH (a)((b)-[*2]->(c))+(d) RETURN a\n"
     "       |           ^^^^^^^^^^^^^\n"
     "-------* A relationship takes one length quantifier: either [e*1..3] inside the brackets or {1,3} after them"},
    {"MATCH (a)((b)-->{2}(c)-->(e))+(d) RETURN a",
     "     1 | MATCH (a)((b)-->{2}(c)-->(e))+(d) RETURN a\n"
     "       |           ^^^^^^^^^^^^^^^^^^\n"
     "-------* A quantified pattern cannot repeat a pattern that is quantified itself"},
    {"MATCH (a)((b)-->(c) WHERE b.x > 1){1,2}(d) RETURN a",
     "SINGLE_PART_QUERY\n"
     "PROPERTY `b.x`\n"
     "LITERAL `1`\n"
     "BINARY `b.x > 1`\n"
     "SYMBOL `a`\n"
     "node `(a)`\n"
     "node `(b)`\n"
     "edge `-->`\n"
     "node `(c)`\n"
     "node `(d)`\n"
     "edge `(b)-->(c)`"},
    {"MATCH (a) WHERE (a)-[*2]->{1,3}(b) RETURN a",
     "     1 | MATCH (a) WHERE (a)-[*2]->{1,3}(b) RETURN a\n"
     "       |                           ^^^^^\n"
     "-------* A relationship takes one length quantifier: either [e*1..3] inside the brackets or {1,3} after them"},
    {"MATCH p = (a)-->(b) RETURN p",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `p`\n"
     "node `(a)`\n"
     "edge `-->`\n"
     "node `(b)`"},
    {"MATCH p = 5",
     "     1 | MATCH p = 5\n"
     "       |           ^\n"
     "-------* syntax error, unexpected DIGIT, expecting OPAREN"},
    {"MATCH p",
     "     1 | MATCH p\n"
     "       |       ^\n"
     "-------* syntax error, unexpected PROG_END, expecting ASSIGN"},
};

const ParseCase PARENTHESIZED_ATOMS[] = {
    {"RETURN (1)-->(b)",
     "     1 | RETURN (1)-->(b)\n"
     "       |        ^\n"
     "-------* Invalid path expr. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'"},
    {"RETURN (1)-->(b) 5",
     "     1 | RETURN (1)-->(b) 5\n"
     "       |                  ^\n"
     "-------* syntax error, unexpected DIGIT"},
    {"RETURN (1)-->(b)-->(c)",
     "     1 | RETURN (1)-->(b)-->(c)\n"
     "       |        ^\n"
     "-------* Invalid path expr. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'"},
    {"RETURN (n.x)-->(b)",
     "     1 | RETURN (n.x)-->(b)\n"
     "       |        ^\n"
     "-------* Invalid path expr. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'"},
    {"RETURN (n:A)-->(b)",
     "SINGLE_PART_QUERY\n"
     "ENTITY_TYPES `n:A`\n"
     "EXISTS `(n:A)-->(b)`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "node `(n:A)-->(b)`"},
    {"RETURN (n {a: 1})-->(b)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "SYMBOL `n`\n"
     "MAP_PROJECTION `n {a: 1}`\n"
     "EXISTS `(n {a: 1})-->(b)`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "node `(n {a: 1})-->(b)`"},
    {"RETURN ({a: 1})-->(b)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `{a: 1}`\n"
     "EXISTS `({a: 1})-->(b)`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "node `({a: 1})-->(b)`"},
    {"RETURN (n {.x})-->(b)",
     "     1 | RETURN (n {.x})-->(b)\n"
     "       |        ^\n"
     "-------* Invalid path expr. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'"},
    {"RETURN (n {.x})-->(b) 5",
     "     1 | RETURN (n {.x})-->(b) 5\n"
     "       |                       ^\n"
     "-------* syntax error, unexpected DIGIT"},
    {"RETURN (n {.x} WHERE true)",
     "     1 | RETURN (n {.x} WHERE true)\n"
     "       |           ^^^^\n"
     "-------* Invalid node pattern. Its properties are 'key: value' entries"},
    {"RETURN (n {.x} WHERE true) 5",
     "     1 | RETURN (n {.x} WHERE true) 5\n"
     "       |           ^^^^\n"
     "-------* Invalid node pattern. Its properties are 'key: value' entries"},
    {"RETURN (n {.x} WHERE true).y",
     "     1 | RETURN (n {.x} WHERE true).y\n"
     "       |           ^^^^\n"
     "-------* Invalid node pattern. Its properties are 'key: value' entries"},
    {"RETURN (n {a: 1} WHERE true)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `true`\n"
     "EXISTS `(n {a: 1} WHERE true)`\n"
     "node `n {a: 1} WHERE true`"},
    {"RETURN ({a: 1} WHERE true)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `true`\n"
     "EXISTS `({a: 1} WHERE true)`\n"
     "node `{a: 1} WHERE true`"},
    {"RETURN ({a: 1})",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `({a: 1})`"},
    {"RETURN (n:A|B WHERE true)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `true`\n"
     "EXISTS `(n:A|B WHERE true)`\n"
     "node `n:A|B WHERE true`"},
    {"RETURN (n:(A|B) WHERE true)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `true`\n"
     "EXISTS `(n:(A|B) WHERE true)`\n"
     "node `n:(A|B) WHERE true`"},
    {"RETURN (n:!A&B WHERE true)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `true`\n"
     "EXISTS `(n:!A&B WHERE true)`\n"
     "node `n:!A&B WHERE true`"},
    {"RETURN (n:$x WHERE true)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `true`\n"
     "EXISTS `(n:$x WHERE true)`\n"
     "node `n:$x WHERE true`"},
    {"RETURN (n:$1 {a: 1})",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "EXISTS `(n:$1 {a: 1})`\n"
     "node `n:$1 {a: 1}`"},
    {"RETURN (n:A)",
     "SINGLE_PART_QUERY\n"
     "ENTITY_TYPES `(n:A)`"},
    {"RETURN (n:A {a: 1})-->(m)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "EXISTS `(n:A {a: 1})-->(m)`\n"
     "node `n:A {a: 1}`\n"
     "edge `-->`\n"
     "node `(m)`"},
    {"RETURN (count {MATCH (n)})",
     "SINGLE_PART_QUERY\n"
     "COUNT_SUBQUERY `(count {MATCH (n)})`\n"
     "node `(n)`"},
    {"RETURN (not where)",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `where`\n"
     "UNARY `(not where)`"},
    {"RETURN (count WHERE x)",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `x`\n"
     "EXISTS `(count WHERE x)`\n"
     "node `count WHERE x`"},
    {"RETURN (null WHERE x)",
     "     1 | RETURN (null WHERE x)\n"
     "       |              ^^^^^\n"
     "-------* syntax error, unexpected WHERE"},
    {"RETURN (true)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `(true)`"},
    {"RETURN (-1, 2)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `(-1, 2)`"},
    {"RETURN (1, 2.5, -3)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `(1, 2.5, -3)`"},
    {"RETURN (1)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `(1)`"},
    {"RETURN (-1)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `(-1)`"},
    {"RETURN (- 1, 2)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `(- 1, 2)`"},
    {"RETURN (1 + 2, 3)",
     "     1 | RETURN (1 + 2, 3)\n"
     "       |              ^\n"
     "-------* syntax error, unexpected COMMA"},
    {"RETURN ()",
     "SINGLE_PART_QUERY\n"
     "EXISTS `()`\n"
     "node ``"},
    {"RETURN ()-->()",
     "SINGLE_PART_QUERY\n"
     "EXISTS `()-->()`\n"
     "node ``\n"
     "edge `-->`\n"
     "node `()`"},
    {"RETURN (WHERE x)",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `x`\n"
     "EXISTS `(WHERE x)`\n"
     "node `WHERE x`"},
    {"RETURN (:A)",
     "SINGLE_PART_QUERY\n"
     "EXISTS `(:A)`\n"
     "node `:A`"},
    {"RETURN (IS A)",
     "SINGLE_PART_QUERY\n"
     "EXISTS `(IS A)`\n"
     "node `IS A`"},
    {"RETURN (is AND x)",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `is`\n"
     "SYMBOL `x`\n"
     "LOGICAL `(is AND x)`"},
    {"RETURN (where + 1)",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `where`\n"
     "LITERAL `1`\n"
     "BINARY `(where + 1)`"},
    {"RETURN (where(1))",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "FUNCTION_INVOCATION `(where(1))`"},
    {"RETURN (where {.x})",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `where`\n"
     "MAP_PROJECTION `(where {.x})`"},
    {"RETURN (where WHERE true)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `true`\n"
     "EXISTS `(where WHERE true)`\n"
     "node `where WHERE true`"},
    {"RETURN (n {a: 1, b: 2",
     "     1 | RETURN (n {a: 1, b: 2\n"
     "       |                     ^\n"
     "-------* syntax error, unexpected PROG_END"},
    {"RETURN (n:A|B",
     "     1 | RETURN (n:A|B\n"
     "       |             ^\n"
     "-------* syntax error, unexpected PROG_END"},
    {"RETURN (n:(A|B",
     "     1 | RETURN (n:(A|B\n"
     "       |              ^\n"
     "-------* syntax error, unexpected PROG_END, expecting CPAREN or COLON or PIPE or AMPERSAND"},
    {"RETURN ({a: (1)} WHERE x)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "SYMBOL `x`\n"
     "EXISTS `({a: (1)} WHERE x)`\n"
     "node `{a: (1)} WHERE x`"},
    {"RETURN (x).name",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `x`\n"
     "PROPERTY `(x).name`"},
    {"RETURN (1).name",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "PROPERTY_LOOKUP `(1).name`"},
    {"RETURN (x.y).name",
     "SINGLE_PART_QUERY\n"
     "PROPERTY `x.y`\n"
     "PROPERTY_LOOKUP `(x.y).name`"},
    {"RETURN ((x)).name",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `x`\n"
     "PROPERTY `((x)).name`"},
};

const ParseCase LABELS_AND_COMPREHENSIONS[] = {
    {"RETURN n:A[0]",
     "SINGLE_PART_QUERY\n"
     "ENTITY_TYPES `n:A`\n"
     "LITERAL `0`\n"
     "INDEX `n:A[0]`"},
    {"RETURN n:A.x",
     "     1 | RETURN n:A.x\n"
     "       |           ^\n"
     "-------* syntax error, unexpected DOT"},
    {"RETURN n.x:A",
     "SINGLE_PART_QUERY\n"
     "PROPERTY `n.x`\n"
     "ENTITY_TYPES `n.x:A`"},
    {"RETURN n[0]:A",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `n`\n"
     "LITERAL `0`\n"
     "INDEX `n[0]`\n"
     "ENTITY_TYPES `n[0]:A`"},
    {"RETURN n.x.y[1..2][..3][4..][..]",
     "SINGLE_PART_QUERY\n"
     "PROPERTY `n.x.y`\n"
     "LITERAL `1`\n"
     "LITERAL `2`\n"
     "LIST_SLICE `n.x.y[1..2]`\n"
     "LITERAL `3`\n"
     "LIST_SLICE `n.x.y[1..2][..3]`\n"
     "LITERAL `4`\n"
     "LIST_SLICE `n.x.y[1..2][..3][4..]`\n"
     "LIST_SLICE `n.x.y[1..2][..3][4..][..]`"},
    {"RETURN [x IN [1] WHERE x:A|B]",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `x`\n"
     "LITERAL `1`\n"
     "LITERAL `[1]`\n"
     "BINARY `x IN [1]`\n"
     "ENTITY_TYPES `x:A`\n"
     "SYMBOL `B`\n"
     "LIST_COMPREHENSION `[x IN [1] WHERE x:A|B]`"},
    {"RETURN [n:A|B]",
     "     1 | RETURN [n:A|B]\n"
     "       |         ^^^\n"
     "-------* Invalid comprehension. Expected 'variable IN list', or a path '(...)-[...]-(...)' followed by '|'"},
    {"RETURN [1, n:A|B]",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "ENTITY_TYPES `n:A|B`\n"
     "LITERAL `[1, n:A|B]`"},
    {"RETURN [x IN xs WHERE x:A|B | x]",
     "     1 | RETURN [x IN xs WHERE x:A|B | x]\n"
     "       |                             ^\n"
     "-------* syntax error, unexpected PIPE"},
    {"RETURN [(a:A|B)-->(b) | b]",
     "SINGLE_PART_QUERY\n"
     "ENTITY_TYPES `a:A|B`\n"
     "EXISTS `(a:A|B)-->(b)`\n"
     "SYMBOL `b`\n"
     "PATTERN_COMPREHENSION `[(a:A|B)-->(b) | b]`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "node `(a:A|B)-->(b)`"},
    {"RETURN reduce(a = 0, x IN n:A|B | a)",
     "     1 | RETURN reduce(a = 0, x IN n:A|B | a)\n"
     "       |                                 ^\n"
     "-------* syntax error, unexpected PIPE"},
    {"MATCH (n:A|B) RETURN n",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `n`\n"
     "node `(n:A|B)`"},
    {"RETURN [x IN [1] | x]",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `x`\n"
     "LITERAL `1`\n"
     "LITERAL `[1]`\n"
     "BINARY `x IN [1]`\n"
     "SYMBOL `x`\n"
     "LIST_COMPREHENSION `[x IN [1] | x]`"},
    {"RETURN [x IN [1]]",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `x`\n"
     "LITERAL `1`\n"
     "LITERAL `[1]`\n"
     "BINARY `x IN [1]`\n"
     "LIST_COMPREHENSION `[x IN [1]]`"},
    {"RETURN [1 | 2]",
     "     1 | RETURN [1 | 2]\n"
     "       |         ^\n"
     "-------* Invalid comprehension. Expected 'variable IN list', or a path '(...)-[...]-(...)' followed by '|'"},
    {"RETURN []",
     "SINGLE_PART_QUERY\n"
     "LITERAL `[]`"},
    {"RETURN [1, 2",
     "     1 | RETURN [1, 2\n"
     "       |            ^\n"
     "-------* syntax error, unexpected PROG_END"},
    {"RETURN [p = (a)-->(b) | p]",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `p`\n"
     "SYMBOL `a`\n"
     "EXISTS `(a)-->(b)`\n"
     "BINARY `p = (a)-->(b)`\n"
     "SYMBOL `p`\n"
     "PATTERN_COMPREHENSION `[p = (a)-->(b) | p]`\n"
     "edge `-->`\n"
     "node `(b)`\n"
     "node `(a)-->(b)`"},
};

const ParseCase KEYWORDS_AS_NAMES[] = {
    {"RETURN case AS x",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `case AS x`"},
    {"RETURN not AS x",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `not AS x`"},
    {"RETURN distinct AS x",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `distinct AS x`"},
    {"RETURN DISTINCT *",
     "SINGLE_PART_QUERY"},
    {"RETURN DISTINCT",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `DISTINCT`"},
    {"RETURN distinct",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `distinct`"},
    {"RETURN count(DISTINCT)",
     "     1 | RETURN count(DISTINCT)\n"
     "       |        ^^^^^^^^^^^^^^^\n"
     "-------* Not implemented: count(DISTINCT)"},
    {"RETURN toUpper(DISTINCT)",
     "     1 | RETURN toUpper(DISTINCT)\n"
     "       |        ^^^^^^^^^^^^^^^^^\n"
     "-------* DISTINCT requires an argument"},
    {"RETURN collect(DISTINCT)",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `DISTINCT`\n"
     "FUNCTION_INVOCATION `collect(DISTINCT)`"},
    {"RETURN count()",
     "     1 | RETURN count()\n"
     "       |        ^^^^^^^\n"
     "-------* Not implemented: count()"},
    {"RETURN count(*)",
     "SINGLE_PART_QUERY\n"
     "LITERAL (no location)\n"
     "FUNCTION_INVOCATION `count(*)`"},
    {"RETURN count(DISTINCT 1, 2)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `2`\n"
     "FUNCTION_INVOCATION `count(DISTINCT 1, 2)`"},
    {"RETURN f(DISTINCT 1)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "FUNCTION_INVOCATION `f(DISTINCT 1)`"},
    {"RETURN collect(DISTINCT as)",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `as`\n"
     "FUNCTION_INVOCATION `collect(DISTINCT as)`"},
    {"MATCH (n) WHERE CASE as WHEN 1 THEN true END RETURN n",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `as`\n"
     "LITERAL `1`\n"
     "LITERAL `true`\n"
     "CASE `CASE as WHEN 1 THEN true END`\n"
     "SYMBOL `n`\n"
     "node `(n)`"},
    {"ORDER BY case ASC",
     "     1 | ORDER BY case ASC\n"
     "       | ^^^^^\n"
     "-------* syntax error, unexpected ORDER"},
    {"MATCH (n) RETURN n ORDER BY case ASC",
     "     1 | MATCH (n) RETURN n ORDER BY case ASC\n"
     "       |                                  ^^^\n"
     "-------* syntax error, unexpected PROG_END"},
    {"UNWIND not AS x RETURN x",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `not`\n"
     "SYMBOL `x`"},
    {"UNWIND case AS x RETURN x",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `case`\n"
     "SYMBOL `x`"},
    {"RETURN (case AS x)",
     "     1 | RETURN (case AS x)\n"
     "       |                 ^\n"
     "-------* syntax error, unexpected ID"},
    {"RETURN exists",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `exists`"},
};

const ParseCase DETAILS_AND_READING_CLAUSES[] = {
    {"LOAD CSV",
     "     1 | LOAD CSV\n"
     "       |      ^^^\n"
     "-------* syntax error, unexpected PROG_END, expecting STRING_LITERAL"},
    {"LOAD CSV 'f' AS r RETURN r",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `r`"},
    {"LOAD CSV 'f' WITH HEADERS ON ERROR SKIP AS r RETURN r",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `r`"},
    {"LOAD CSV 'f' ON ERROR 5",
     "     1 | LOAD CSV 'f' ON ERROR 5\n"
     "       |                       ^\n"
     "-------* syntax error, unexpected DIGIT, expecting SKIP or FAIL"},
    {"MATCH (WHERE true) RETURN 1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `true`\n"
     "LITERAL `1`\n"
     "node `(WHERE true)`"},
    {"MATCH (where {a: 1}) RETURN where",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "SYMBOL `where`\n"
     "node `(where {a: 1})`"},
    {"MATCH (IS AND) RETURN 1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "node `(IS AND)`"},
    {"MATCH (is:Person) RETURN is",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `is`\n"
     "node `(is:Person)`"},
    {"MATCH (where) RETURN where",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `where`\n"
     "node `(where)`"},
    {"MATCH (is) RETURN is",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `is`\n"
     "node `(is)`"},
    {"MATCH (is IS A) RETURN is",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `is`\n"
     "node `(is IS A)`"},
    {"MATCH (null) RETURN 1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "node `(null)`"},
    {"MATCH (true:A) RETURN 1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "node `(true:A)`"},
    {"MATCH ()-[where]-() RETURN 1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "node `()`\n"
     "edge `-[where]-`\n"
     "node `()`"},
    {"MATCH ()-[WHERE true]-() RETURN 1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `true`\n"
     "LITERAL `1`\n"
     "node `()`\n"
     "edge `-[WHERE true]-`\n"
     "node `()`"},
    {"MATCH ()-[IS T]-() RETURN 1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "node `()`\n"
     "edge `-[IS T]-`\n"
     "node `()`"},
    {"MATCH ()-[is]-() RETURN 1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "node `()`\n"
     "edge `-[is]-`\n"
     "node `()`"},
    {"MATCH ()-[where {a: 1}]-() RETURN 1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `1`\n"
     "node `()`\n"
     "edge `-[where {a: 1}]-`\n"
     "node `()`"},
    {"MATCH (n) WITH n SKIP 1 LIMIT 2 WHERE n.x > 1 RETURN n",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `n`\n"
     "LITERAL `1`\n"
     "LITERAL `2`\n"
     "PROPERTY `n.x`\n"
     "LITERAL `1`\n"
     "BINARY `n.x > 1`\n"
     "SYMBOL `n`\n"
     "node `(n)`"},
    {"MATCH (n) RETURN n SKIP 1 LIMIT 2 ORDER BY n",
     "     1 | MATCH (n) RETURN n SKIP 1 LIMIT 2 ORDER BY n\n"
     "       |                                   ^^^^^\n"
     "-------* syntax error, unexpected ORDER"},
    {"MATCH (n) RETURN n ORDER BY n.x DESC, n.y ASCENDING SKIP 1",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `n`\n"
     "PROPERTY `n.x`\n"
     "PROPERTY `n.y`\n"
     "LITERAL `1`\n"
     "node `(n)`"},
    {"MATCH (n) SHORTESTPATH(a, b, w, d, p) RETURN d",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `d`\n"
     "node `(n)`"},
    {"MATCH (n) SHORTESTPATH(a, b, null, d, p) RETURN d",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `d`\n"
     "node `(n)`"},
    {"MATCH (n) SHORTESTPATH(a, b, w, d, p) MATCH (m) RETURN d",
     "     1 | MATCH (n) SHORTESTPATH(a, b, w, d, p) MATCH (m) RETURN d\n"
     "       |                                       ^^^^^\n"
     "-------* syntax error, unexpected MATCH, expecting RETURN"},
    {"VECTOR SEARCH IN v FOR 3 (1, 2) YIELD ids RETURN ids",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `ids`\n"
     "SYMBOL `ids`"},
    {"VECTOR SEARCH IN v FOR 3 (1) YIELD ids",
     "     1 | VECTOR SEARCH IN v FOR 3 (1) YIELD ids\n"
     "       |                            ^\n"
     "-------* syntax error, unexpected CPAREN, expecting COMMA"},
    {"VECTOR SEARCH IN v FOR 3 (-1.5, 2e3) YIELD *",
     "SINGLE_PART_QUERY"},
    {"MATCH (n IS NULL) RETURN n",
     "     1 | MATCH (n IS NULL) RETURN n\n"
     "       |          ^^\n"
     "-------* syntax error, unexpected IS_VALUE_TEST"},
};

const ParseCase LITERALS_AND_OPERATORS[] = {
    {"RETURN $1, $a, $null, $5x",
     "     1 | RETURN $1, $a, $null, $5x\n"
     "       |                         ^\n"
     "-------* syntax error, unexpected ID"},
    {"RETURN $",
     "     1 | RETURN $\n"
     "       |        ^\n"
     "-------* syntax error, unexpected PROG_END, expecting a name or DIGIT"},
    {"RETURN 9223372036854775808",
     "     1 | RETURN 9223372036854775808\n"
     "       |        ^^^^^^^^^^^^^^^^^^^\n"
     "-------* syntax error, unexpected integer literal 9223372036854775808, which exceeds int64 range unless negated"},
    {"RETURN -9223372036854775808",
     "SINGLE_PART_QUERY\n"
     "LITERAL `-9223372036854775808`"},
    {"RETURN --9223372036854775808",
     "SINGLE_PART_QUERY\n"
     "LITERAL `-9223372036854775808`\n"
     "UNARY `--9223372036854775808`"},
    {"RETURN - -1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `-1`\n"
     "LITERAL `- -1`"},
    {"RETURN -+-1",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `-1`\n"
     "UNARY `+-1`\n"
     "UNARY `-+-1`"},
    {"RETURN NOT NOT true",
     "SINGLE_PART_QUERY\n"
     "LITERAL `true`\n"
     "UNARY `NOT true`\n"
     "UNARY `NOT NOT true`"},
    {"RETURN NOT not",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `not`\n"
     "UNARY `NOT not`"},
    {"RETURN 1 + not",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "SYMBOL `not`\n"
     "BINARY `1 + not`"},
    {"RETURN 1 + not x",
     "     1 | RETURN 1 + not x\n"
     "       |                ^\n"
     "-------* syntax error, unexpected ID"},
    {"RETURN 1 + case WHEN true THEN 1 END",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `true`\n"
     "LITERAL `1`\n"
     "CASE `case WHEN true THEN 1 END`\n"
     "BINARY `1 + case WHEN true THEN 1 END`"},
    {"RETURN 0x1F, 0o17, 1.5e3, .5, 1day",
     "     1 | RETURN 0x1F, 0o17, 1.5e3, .5, 1day\n"
     "       |                                 ^^\n"
     "-------* syntax error, unexpected ID"},
    {"RETURN 0x8000000000000000",
     "     1 | RETURN 0x8000000000000000\n"
     "       |        ^^^^^^^^^^^^^^^^^^\n"
     "-------* Hexadecimal literal exceeded int64 range."},
    {"RETURN 0o1777777777777777777777",
     "     1 | RETURN 0o1777777777777777777777\n"
     "       |        ^^^^^^^^^^^^^^^^^^^^^^^^\n"
     "-------* Octal literal exceeded int64 range."},
    {"RETURN 'a' STARTS WITH 'b' ENDS WITH 'c' CONTAINS 'd'",
     "SINGLE_PART_QUERY\n"
     "LITERAL `'a'`\n"
     "LITERAL `'b'`\n"
     "STRING `'a' STARTS WITH 'b'`\n"
     "LITERAL `'c'`\n"
     "STRING `'a' STARTS WITH 'b' ENDS WITH 'c'`\n"
     "LITERAL `'d'`\n"
     "STRING `'a' STARTS WITH 'b' ENDS WITH 'c' CONTAINS 'd'`"},
    {"RETURN 2 ^ 3 ^ 4 * 5 / 6 % 7 + 8 - 9 || 'a'",
     "SINGLE_PART_QUERY\n"
     "LITERAL `2`\n"
     "LITERAL `3`\n"
     "BINARY `2 ^ 3`\n"
     "LITERAL `4`\n"
     "BINARY `2 ^ 3 ^ 4`\n"
     "LITERAL `5`\n"
     "BINARY `2 ^ 3 ^ 4 * 5`\n"
     "LITERAL `6`\n"
     "BINARY `2 ^ 3 ^ 4 * 5 / 6`\n"
     "LITERAL `7`\n"
     "BINARY `2 ^ 3 ^ 4 * 5 / 6 % 7`\n"
     "LITERAL `8`\n"
     "BINARY `2 ^ 3 ^ 4 * 5 / 6 % 7 + 8`\n"
     "LITERAL `9`\n"
     "BINARY `2 ^ 3 ^ 4 * 5 / 6 % 7 + 8 - 9`\n"
     "LITERAL `'a'`\n"
     "BINARY `2 ^ 3 ^ 4 * 5 / 6 % 7 + 8 - 9 || 'a'`"},
    {"RETURN normalize('a', NFD)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `'a'`\n"
     "LITERAL (no location)\n"
     "FUNCTION_INVOCATION `normalize('a', NFD)`"},
    {"RETURN normalize('a', 5)",
     "     1 | RETURN normalize('a', 5)\n"
     "       |                       ^\n"
     "-------* syntax error, unexpected DIGIT, expecting NFC or NFD or NFKC or NFKD"},
    {"RETURN normalize('a',)",
     "     1 | RETURN normalize('a',)\n"
     "       |                      ^\n"
     "-------* syntax error, unexpected CPAREN, expecting NFC or NFD or NFKC or NFKD"},
    {"RETURN all(x IN [1])",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `[1]`\n"
     "LIST_COMPREHENSION `x IN [1]`\n"
     "LIST_PREDICATE `all(x IN [1])`"},
    {"RETURN any(x IN [1] WHERE x > 0)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `[1]`\n"
     "SYMBOL `x`\n"
     "LITERAL `0`\n"
     "BINARY `x > 0`\n"
     "LIST_COMPREHENSION `x IN [1] WHERE x > 0`\n"
     "LIST_PREDICATE `any(x IN [1] WHERE x > 0)`"},
    {"RETURN allReduce(a = 0, x IN [1] | a + x, a > 0)",
     "SINGLE_PART_QUERY\n"
     "LITERAL `0`\n"
     "LITERAL `1`\n"
     "LITERAL `[1]`\n"
     "SYMBOL `a`\n"
     "SYMBOL `x`\n"
     "BINARY `a + x`\n"
     "SYMBOL `a`\n"
     "LITERAL `0`\n"
     "BINARY `a > 0`\n"
     "ALL_REDUCE `allReduce(a = 0, x IN [1] | a + x, a > 0)`"},
    {"RETURN n {.x, .*, y, z: 1}",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `y`\n"
     "LITERAL `1`\n"
     "SYMBOL `n`\n"
     "MAP_PROJECTION `n {.x, .*, y, z: 1}`"},
    {"RETURN n {}",
     "SINGLE_PART_QUERY\n"
     "SYMBOL `n`\n"
     "MAP_PROJECTION `n {}`"},
    {"RETURN n {null}",
     "     1 | RETURN n {null}\n"
     "       |               ^\n"
     "-------* syntax error, unexpected CBRACE, expecting COLON"},
    {"RETURN n {5}",
     "     1 | RETURN n {5}\n"
     "       |           ^\n"
     "-------* syntax error, unexpected DIGIT, expecting CBRACE or DOT or a name"},
    {"RETURN {a: 1, b: {c: [1, {d: 2}]}}",
     "SINGLE_PART_QUERY\n"
     "LITERAL `1`\n"
     "LITERAL `1`\n"
     "LITERAL `2`\n"
     "LITERAL `{d: 2}`\n"
     "LITERAL `[1, {d: 2}]`\n"
     "LITERAL `{c: [1, {d: 2}]}`\n"
     "LITERAL `{a: 1, b: {c: [1, {d: 2}]}}`"},
    {"RETURN {}",
     "SINGLE_PART_QUERY\n"
     "LITERAL `{}`"},
    {"MATCH (n) RETURN n.x IN [1] = true <> false",
     "SINGLE_PART_QUERY\n"
     "PROPERTY `n.x`\n"
     "LITERAL `1`\n"
     "LITERAL `[1]`\n"
     "BINARY `n.x IN [1]`\n"
     "LITERAL `true`\n"
     "BINARY `n.x IN [1] = true`\n"
     "LITERAL `false`\n"
     "BINARY `true <> false`\n"
     "LOGICAL `n.x IN [1] = true <> false`\n"
     "node `(n)`"},
    {"MATCH (n) RETURN n.x STARTS 5",
     "     1 | MATCH (n) RETURN n.x STARTS 5\n"
     "       |                             ^\n"
     "-------* syntax error, unexpected DIGIT, expecting WITH"},
};

}

// Each query stands where the parser decides between two readings, holds an error until the
// next token or reports one. Every query one edit away from it must parse or report an error.
class ParseEdgeCaseTest : public ::testing::Test {
protected:
    static void expectOutcomes(std::span<const ParseCase> cases) {
        for (const ParseCase& parseCase : cases) {
            std::string outcome;
            renderParseOutcome(parseCase._query, outcome);
            EXPECT_EQ(outcome, parseCase._outcome) << parseCase._query;

            std::vector<std::string> mutants;
            buildQueryMutants(std::string(parseCase._query), mutants);

            for (const std::string& mutant : mutants) {
                std::string problem;
                EXPECT_TRUE(isWellFormedParse(mutant, problem)) << mutant << "\n" << problem;
            }
        }
    }
};

TEST_F(ParseEdgeCaseTest, parsesScripts) {
    expectOutcomes(SCRIPTS);
}

TEST_F(ParseEdgeCaseTest, parsesAdministrativeQueries) {
    expectOutcomes(ADMINISTRATIVE_QUERIES);
}

TEST_F(ParseEdgeCaseTest, parsesExplainOptions) {
    expectOutcomes(EXPLAIN_OPTIONS);
}

TEST_F(ParseEdgeCaseTest, parsesUnionsNextChainsAndConditionalQueries) {
    expectOutcomes(QUERY_COMPOSITIONS);
}

TEST_F(ParseEdgeCaseTest, parsesCallsAndSubqueries) {
    expectOutcomes(CALLS_AND_SUBQUERIES);
}

TEST_F(ParseEdgeCaseTest, parsesValueTestsAndCases) {
    expectOutcomes(VALUE_TESTS_AND_CASES);
}

TEST_F(ParseEdgeCaseTest, parsesUpdatingClauses) {
    expectOutcomes(UPDATING_CLAUSES);
}

TEST_F(ParseEdgeCaseTest, parsesRelationships) {
    expectOutcomes(RELATIONSHIPS);
}

TEST_F(ParseEdgeCaseTest, parsesParenthesizedAtoms) {
    expectOutcomes(PARENTHESIZED_ATOMS);
}

TEST_F(ParseEdgeCaseTest, parsesLabelExpressionsAndComprehensions) {
    expectOutcomes(LABELS_AND_COMPREHENSIONS);
}

TEST_F(ParseEdgeCaseTest, parsesKeywordsUsedAsNames) {
    expectOutcomes(KEYWORDS_AS_NAMES);
}

TEST_F(ParseEdgeCaseTest, parsesEntityDetailsAndReadingClauses) {
    expectOutcomes(DETAILS_AND_READING_CLAUSES);
}

TEST_F(ParseEdgeCaseTest, parsesLiteralsAndOperators) {
    expectOutcomes(LITERALS_AND_OPERATORS);
}
