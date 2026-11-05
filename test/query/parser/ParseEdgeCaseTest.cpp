#include <gtest/gtest.h>

#include <string>
#include <utility>

#include "ChangeOp.h"
#include "ChangeQuery.h"
#include "CommitQuery.h"
#include "CreateEdgePropertyIndexQuery.h"
#include "CreateGraphQuery.h"
#include "CreateNodePropertyIndexQuery.h"
#include "CreateVectorIndexQuery.h"
#include "CypherAST.h"
#include "DeleteVectorIndexQuery.h"
#include "DropIndexQuery.h"
#include "ExpectedASTTest.h"
#include "ExplainRequest.h"
#include "InstallExtensionQuery.h"
#include "ListAvailableGraphsQuery.h"
#include "ListGraphQuery.h"
#include "LoadCommitQuery.h"
#include "LoadEmbeddingQuery.h"
#include "LoadGMLQuery.h"
#include "LoadGraphQuery.h"
#include "LoadJsonlQuery.h"
#include "LoadParquetQuery.h"
#include "LoadVectorQuery.h"
#include "MergeDataPartsQuery.h"
#include "S3ConnectQuery.h"
#include "S3TransferQuery.h"
#include "ShowExtensionsQuery.h"
#include "ShowProceduresQuery.h"
#include "ShowVectorIndexesQuery.h"

using namespace db;
using namespace turing::test;

// Each query stands where the parser decides between two readings, holds an error until the
// next token or reports one
class ParseEdgeCaseTest : public ExpectedASTTest {
};

TEST_F(ParseEdgeCaseTest, parsesScripts) {
    expectError("",
                "       | Empty query\n"
                "     1 | \n"
                "       | \n"
                "-------* syntax error, unexpected PROG_END");

    expectError(";",
                "     1 | ;\n"
                "       | ^\n"
                "-------* syntax error, unexpected SEMI_COLON");

    expectParse("RETURN 1;", {query({}, returning({integer(1)}))});

    expectError("RETURN 1;;",
                "     1 | RETURN 1;;\n"
                "       |          ^\n"
                "-------* syntax error, unexpected SEMI_COLON");

    expectParse("RETURN 1; RETURN 2", {query({}, returning({integer(1)})), query({}, returning({integer(2)}))});

    expectError("  ",
                "     1 |   \n"
                "       | ^^\n"
                "-------* syntax error, unexpected PROG_END");

    expectError("/* c */",
                "     1 | /* c */\n"
                "       | ^^^^^^^\n"
                "-------* syntax error, unexpected PROG_END");

    expectError("// c",
                "     1 | // c\n"
                "       | ^^^^\n"
                "-------* syntax error, unexpected PROG_END");

    expectError("RETURN 1 +",
                "     1 | RETURN 1 +\n"
                "       |          ^\n"
                "-------* syntax error, unexpected PROG_END");

    expectParse("RETURN 1; RETURN 2; RETURN 3",
                {query({}, returning({integer(1)})),
                 query({}, returning({integer(2)})),
                 query({}, returning({integer(3)}))});

    expectParse("CREATE (n:A); MATCH (n:A) RETURN n",
                {query({createStmt(pattern({path({node({._symbol = "n", ._labels = labels({"A"})})})}))}),
                 query({match({._pattern = pattern({path({node({._symbol = "n", ._labels = labels({"A"})})})})})},
                       returning({symbol("n")}))});

    expectParse("LOAD GRAPH g; MATCH (n) RETURN n",
                {LoadGraphQuery::create(getExpectedAST(), "g"),
                 query({match({._pattern = pattern({path({node({._symbol = "n"})})})})}, returning({symbol("n")}))});

    expectParse("RETURN 1;\nRETURN 2", {query({}, returning({integer(1)})), query({}, returning({integer(2)}))});

    expectParse("RETURN 1; /* c */ RETURN 2;", {query({}, returning({integer(1)})), query({}, returning({integer(2)}))});

    getExpectedAST()->explainRequest().requestDefaults();
    expectParse("EXPLAIN RETURN 1; RETURN 2", {query({}, returning({integer(1)})), query({}, returning({integer(2)}))});

    expectError("RETURN 1; RETURN 2 +",
                "     1 | RETURN 1; RETURN 2 +\n"
                "       |                    ^\n"
                "-------* syntax error, unexpected PROG_END");
}

TEST_F(ParseEdgeCaseTest, parsesAdministrativeQueries) {
    expectError("CREATE GRAPH",
                "     1 | CREATE GRAPH\n"
                "       |        ^^^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting ASSIGN or a name");

    expectError("CREATE GRAPH 99999999999999999999",
                "     1 | CREATE GRAPH 99999999999999999999\n"
                "       |              ^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Decimal literal exceeded int64 range.");

    expectParse("CREATE GRAPH g", {CreateGraphQuery::create(getExpectedAST(), "g")});

    expectParse("CREATE GRAPH = (a)", {query({createStmt(pattern({namedPath("GRAPH", {node({._symbol = "a"})})}))})});

    expectError("CREATE CONSTRAINT",
                "     1 | CREATE CONSTRAINT\n"
                "       |        ^^^^^^^^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting ASSIGN or a name");

    expectError("CREATE CONSTRAINT null",
                "     1 | CREATE CONSTRAINT null\n"
                "       |                   ^^^^\n"
                "-------* syntax error, unexpected NULL_, expecting ASSIGN or a name");

    expectError("CREATE CONSTRAINT c FOR (5",
                "     1 | CREATE CONSTRAINT c FOR (5\n"
                "       |                          ^\n"
                "-------* syntax error, unexpected DIGIT, expecting CPAREN or a name");

    expectError("CREATE CONSTRAINT c FOR (",
                "     1 | CREATE CONSTRAINT c FOR (\n"
                "       |                         ^\n"
                "-------* syntax error, unexpected PROG_END, expecting CPAREN or a name");

    expectError("CREATE CONSTRAINT c FOR (n:P) REQUIRE n.x IS UNIQUE",
                "     1 | CREATE CONSTRAINT c FOR (n:P) REQUIRE n.x IS UNIQUE\n"
                "       | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Not implemented: CREATE CONSTRAINT");

    expectError("CREATE CONSTRAINT c IF NOT EXISTS FOR (n:P) REQUIRE n.x IS UNIQUE 5",
                "     1 | CREATE CONSTRAINT c IF NOT EXISTS FOR (n:P) REQUIRE n.x IS UNIQUE 5\n"
                "       | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Not implemented: CREATE CONSTRAINT");

    expectError("CREATE CONSTRAINT c FOR ()-[r:T]-() REQUIRE r.x IS NOT NULL",
                "     1 | CREATE CONSTRAINT c FOR ()-[r:T]-() REQUIRE r.x IS NOT NULL\n"
                "       | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Not implemented: CREATE CONSTRAINT");

    expectError("CREATE CONSTRAINT c FOR (n:P) REQUIRE n.x IS NOT 5",
                "     1 | CREATE CONSTRAINT c FOR (n:P) REQUIRE n.x IS NOT 5\n"
                "       |                                                  ^\n"
                "-------* syntax error, unexpected DIGIT, expecting NULL_");

    expectError("CREATE VECTOR",
                "     1 | CREATE VECTOR\n"
                "       |        ^^^^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting ASSIGN or INDEX");

    expectError("CREATE VECTOR 5",
                "     1 | CREATE VECTOR 5\n"
                "       |               ^\n"
                "-------* syntax error, unexpected DIGIT, expecting ASSIGN or INDEX");

    expectParse("CREATE VECTOR = (a)", {query({createStmt(pattern({namedPath("VECTOR", {node({._symbol = "a"})})}))})});

    expectParse("CREATE VECTOR INDEX v WITH DIMENSION 4 METRIC EUCLID",
                {CreateVectorIndexQuery::create(getExpectedAST(),
                                                "v",
                                                4,
                                                vec::DistanceMetric::EUCLIDEAN_DIST,
                                                vec::IndexType::FLAT)});

    expectParse("CREATE VECTOR INDEX v WITH DIMENSION 4 METRIC COSINE TYPE HNSW",
                {CreateVectorIndexQuery::create(getExpectedAST(),
                                                "v",
                                                4,
                                                vec::DistanceMetric::INNER_PRODUCT,
                                                vec::IndexType::HNSW)});

    expectError("CREATE VECTOR INDEX v WITH DIMENSION 4 METRIC X",
                "     1 | CREATE VECTOR INDEX v WITH DIMENSION 4 METRIC X\n"
                "       |                                               ^\n"
                "-------* syntax error, unexpected ID, expecting EUCLID or COSINE");

    expectError("CREATE INDEX",
                "     1 | CREATE INDEX\n"
                "       |        ^^^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting ASSIGN or a name");

    expectError("CREATE INDEX i",
                "     1 | CREATE INDEX i\n"
                "       |              ^\n"
                "-------* syntax error, unexpected PROG_END, expecting FOR");

    expectError("CREATE INDEX i 5",
                "     1 | CREATE INDEX i 5\n"
                "       |                ^\n"
                "-------* syntax error, unexpected DIGIT, expecting FOR");

    expectError("CREATE INDEX i FOR",
                "     1 | CREATE INDEX i FOR\n"
                "       |                ^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting OPAREN or OBRACK");

    expectError("CREATE INDEX i FOR 5",
                "     1 | CREATE INDEX i FOR 5\n"
                "       |                    ^\n"
                "-------* syntax error, unexpected DIGIT, expecting OPAREN or OBRACK");

    expectParse("CREATE INDEX i FOR (n:P) ON n.x",
                {CreateNodePropertyIndexQuery::create(getExpectedAST(),
                                                      "i",
                                                      node({._symbol = "n", ._labels = labels({"P"})}),
                                                      property({"n", "x"}))});

    expectError("CREATE INDEX i FOR (n:P) ON n.x 5",
                "     1 | CREATE INDEX i FOR (n:P) ON n.x 5\n"
                "       |                                 ^\n"
                "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON or DOT");

    expectError("CREATE INDEX i FOR (n:P) ON (n).x 5",
                "     1 | CREATE INDEX i FOR (n:P) ON (n).x 5\n"
                "       |                                   ^\n"
                "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON");

    expectError("CREATE INDEX i FOR (n WHERE n.x > 1) ON n.x",
                "     1 | CREATE INDEX i FOR (n WHERE n.x > 1) ON n.x\n"
                "       |                    ^^^^^^^^^^^^^^^^^\n"
                "-------* WHERE is not allowed in an index pattern");

    expectError("CREATE INDEX i FOR (n WHERE n.x > 1) ON n.x 5",
                "     1 | CREATE INDEX i FOR (n WHERE n.x > 1) ON n.x 5\n"
                "       |                                             ^\n"
                "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON or DOT");

    expectError("CREATE INDEX i FOR (n WHERE n.x > 1) ON (n).x 5",
                "     1 | CREATE INDEX i FOR (n WHERE n.x > 1) ON (n).x 5\n"
                "       |                    ^^^^^^^^^^^^^^^^^\n"
                "-------* WHERE is not allowed in an index pattern");

    expectError("CREATE INDEX i FOR (n WHERE n.x > 1) ON (n).x;",
                "     1 | CREATE INDEX i FOR (n WHERE n.x > 1) ON (n).x;\n"
                "       |                    ^^^^^^^^^^^^^^^^^\n"
                "-------* WHERE is not allowed in an index pattern");

    expectParse("CREATE INDEX i FOR [e:T] ON e.x",
                {CreateEdgePropertyIndexQuery::create(getExpectedAST(),
                                                      "i",
                                                      edge({._symbol = "e", ._labels = labels({"T"})}),
                                                      property({"e", "x"}))});

    expectParse("CREATE INDEX i FOR [] ON e.x",
                {CreateEdgePropertyIndexQuery::create(getExpectedAST(), "i", edge({}), property({"e", "x"}))});

    expectError("CREATE INDEX i FOR [e WHERE e.x > 1] ON e.x",
                "     1 | CREATE INDEX i FOR [e WHERE e.x > 1] ON e.x\n"
                "       |                     ^^^^^^^^^^^^^^^\n"
                "-------* WHERE is not allowed in an index pattern");

    expectError("CREATE INDEX i FOR [e*1..2] ON e.x",
                "     1 | CREATE INDEX i FOR [e*1..2] ON e.x\n"
                "       |                     ^^^^^^\n"
                "-------* An edge property index takes no length quantifier");

    expectError("CREATE INDEX i FOR [e*1..2 WHERE e.x > 1] ON e.x.y",
                "     1 | CREATE INDEX i FOR [e*1..2 WHERE e.x > 1] ON e.x.y\n"
                "       |                     ^^^^^^^^^^^^^^^^^^^^\n"
                "-------* WHERE is not allowed in an index pattern");

    expectError("CREATE INDEX i FOR (n) ON (1).x",
                "     1 | CREATE INDEX i FOR (n) ON (1).x\n"
                "       |                           ^^^\n"
                "-------* Invalid property access. Only a variable can be parenthesized on the left of '.'");

    expectError("CREATE INDEX i FOR (n) ON (n.y).x",
                "     1 | CREATE INDEX i FOR (n) ON (n.y).x\n"
                "       |                           ^^^^^\n"
                "-------* Invalid property access. Only a variable can be parenthesized on the left of '.'");

    expectError("CREATE INDEX i FOR (n) ON 99999999999999999999",
                "     1 | CREATE INDEX i FOR (n) ON 99999999999999999999\n"
                "       |                           ^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Decimal literal exceeded int64 range.");

    expectError("DROP",
                "     1 | DROP\n"
                "       | ^^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting CONSTRAINT or INDEX");

    expectError("DROP 5",
                "     1 | DROP 5\n"
                "       |      ^\n"
                "-------* syntax error, unexpected DIGIT, expecting CONSTRAINT or INDEX");

    expectError("DROP INDEX",
                "     1 | DROP INDEX\n"
                "       |      ^^^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting a name");

    expectParse("DROP INDEX i", {DropIndexQuery::create(getExpectedAST(), "i")});

    expectError("DROP CONSTRAINT foo",
                "     1 | DROP CONSTRAINT foo\n"
                "       | ^^^^^^^^^^^^^^^^^^^\n"
                "-------* Not implemented: DROP CONSTRAINT");

    expectError("DROP CONSTRAINT foo 5",
                "     1 | DROP CONSTRAINT foo 5\n"
                "       |                     ^\n"
                "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON or IF");

    expectError("DROP CONSTRAINT foo;",
                "     1 | DROP CONSTRAINT foo;\n"
                "       | ^^^^^^^^^^^^^^^^^^^\n"
                "-------* Not implemented: DROP CONSTRAINT");

    expectError("DROP CONSTRAINT foo IF EXISTS",
                "     1 | DROP CONSTRAINT foo IF EXISTS\n"
                "       | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Not implemented: DROP CONSTRAINT");

    expectError("DROP CONSTRAINT foo IF EXISTS 5",
                "     1 | DROP CONSTRAINT foo IF EXISTS 5\n"
                "       | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Not implemented: DROP CONSTRAINT");

    expectError("DROP CONSTRAINT foo IF 5",
                "     1 | DROP CONSTRAINT foo IF 5\n"
                "       |                        ^\n"
                "-------* syntax error, unexpected DIGIT, expecting EXISTS");

    expectError("LOAD",
                "     1 | LOAD\n"
                "       | ^^^^\n"
                "-------* syntax error, unexpected PROG_END");

    expectError("LOAD 5",
                "     1 | LOAD 5\n"
                "       |      ^\n"
                "-------* syntax error, unexpected DIGIT");

    expectError("LOAD GRAPH",
                "     1 | LOAD GRAPH\n"
                "       |      ^^^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting a name");

    expectParse("LOAD GRAPH g", {LoadGraphQuery::create(getExpectedAST(), "g")});

    expectParse("LOAD JSONL 'f'", {LoadJsonlQuery::create(getExpectedAST(), fs::Path(std::string("f")))});

    {
        LoadJsonlQuery* load = LoadJsonlQuery::create(getExpectedAST(), fs::Path(std::string("f")));
        load->setGraphName("g");

        expectParse("LOAD JSONL 'f' AS g", {load});
    }

    {
        LoadJsonlQuery* load = LoadJsonlQuery::create(getExpectedAST(), fs::Path(std::string("f")));

        EmbeddingsSpec embeddings;
        embeddings.emplace("e", 4);
        load->setEmbeddingSpecs(std::move(embeddings));

        TypedPropertySpec types;
        types.emplace("d", ValueType::DateTime);
        types.emplace("t", ValueType::DateTime);
        types.emplace("u", ValueType::Duration);
        load->setTypedPropertySpec(std::move(types));

        expectParse("LOAD JSONL 'f' WITH EMBEDDINGS [{e, 4}] WITH DATETIMES ['d', t] WITH DURATIONS [u]", {load});
    }

    expectError("LOAD JSONL 'f' WITH 5",
                "     1 | LOAD JSONL 'f' WITH 5\n"
                "       |                     ^\n"
                "-------* syntax error, unexpected DIGIT, expecting DATETIMES or DURATIONS or EMBEDDINGS");

    expectParse("LOAD GML 'f'", {LoadGMLQuery::create(getExpectedAST(), fs::Path(std::string("f")))});

    {
        LoadGMLQuery* load = LoadGMLQuery::create(getExpectedAST(), fs::Path(std::string("f")));
        load->setGraphName("g");

        expectParse("LOAD GML 'f' AS g", {load});
    }

    {
        LoadParquetQuery* load = LoadParquetQuery::create(getExpectedAST(), fs::Path(std::string("f")));
        load->setGraphName("g");

        TypedPropertySpec types;
        types.emplace("d", ValueType::Duration);
        types.emplace("e", ValueType::Duration);
        load->setTypedPropertySpec(std::move(types));

        expectParse("LOAD PARQUET 'f' AS g WITH DURATIONS [d, 'e']", {load});
    }

    expectError("LOAD PARQUET 'f' 5",
                "     1 | LOAD PARQUET 'f' 5\n"
                "       |                  ^\n"
                "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON or WITH or AS");

    expectParse("LOAD COMMIT 'h'", {LoadCommitQuery::create(getExpectedAST(), "h")});

    expectParse("LOAD VECTOR FROM 'f' IN v", {LoadVectorQuery::create(getExpectedAST(), "f", "v")});

    expectParse("LOAD EMBEDDING FROM 'f' AS e", {LoadEmbeddingQuery::create(getExpectedAST(), "f", "e")});

    expectError("LIST",
                "     1 | LIST\n"
                "       | ^^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting GRAPH or AVAILABLE");

    expectParse("LIST GRAPH", {ListGraphQuery::create(getExpectedAST())});

    expectParse("LIST AVAILABLE GRAPHS", {ListAvailableGraphsQuery::create(getExpectedAST())});

    expectError("LIST 5",
                "     1 | LIST 5\n"
                "       |      ^\n"
                "-------* syntax error, unexpected DIGIT, expecting GRAPH or AVAILABLE");

    expectError("SHOW",
                "     1 | SHOW\n"
                "       | ^^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting PROCEDURES or EXTENSIONS or VECTOR");

    expectParse("SHOW PROCEDURES", {ShowProceduresQuery::create(getExpectedAST())});

    expectParse("SHOW EXTENSIONS", {ShowExtensionsQuery::create(getExpectedAST())});

    expectParse("SHOW VECTOR INDEXES", {ShowVectorIndexesQuery::create(getExpectedAST())});

    expectError("SHOW 5",
                "     1 | SHOW 5\n"
                "       |      ^\n"
                "-------* syntax error, unexpected DIGIT, expecting PROCEDURES or EXTENSIONS or VECTOR");

    expectError("S3",
                "     1 | S3\n"
                "       | ^^\n"
                "-------* syntax error, unexpected PROG_END, expecting CONNECT or PUSH or PULL");

    expectParse("S3 CONNECT 'a' 'b' 'c'", {S3ConnectQuery::create(getExpectedAST(), "a", "b", "c")});

    expectParse("S3 PULL 'a' 'b'",
                {S3TransferQuery::create(getExpectedAST(), S3TransferQuery::Direction::PULL, "a", "b")});

    expectParse("S3 PUSH 'a' 'b'",
                {S3TransferQuery::create(getExpectedAST(), S3TransferQuery::Direction::PUSH, "b", "a")});

    expectError("S3 5",
                "     1 | S3 5\n"
                "       |    ^\n"
                "-------* syntax error, unexpected DIGIT, expecting CONNECT or PUSH or PULL");

    expectParse("DELETE VECTOR INDEX v", {DeleteVectorIndexQuery::create(getExpectedAST(), "v")});

    expectParse("DELETE VECTOR", {query({deleteStmt({symbol("VECTOR")})})});

    expectError("DELETE VECTOR 5",
                "     1 | DELETE VECTOR 5\n"
                "       |               ^\n"
                "-------* syntax error, unexpected DIGIT");

    expectParse("CHANGE NEW", {ChangeQuery::create(getExpectedAST(), ChangeOp::NEW)});

    expectParse("CHANGE SUBMIT", {ChangeQuery::create(getExpectedAST(), ChangeOp::SUBMIT)});

    expectParse("CHANGE DELETE", {ChangeQuery::create(getExpectedAST(), ChangeOp::DELETE)});

    expectParse("CHANGE LIST", {ChangeQuery::create(getExpectedAST(), ChangeOp::LIST)});

    expectError("CHANGE 5",
                "     1 | CHANGE 5\n"
                "       |        ^\n"
                "-------* syntax error, unexpected DIGIT, expecting SUBMIT or DELETE or LIST or NEW");

    expectParse("COMMIT", {CommitQuery::create(getExpectedAST())});

    expectError("COMMIT 5",
                "     1 | COMMIT 5\n"
                "       |        ^\n"
                "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON");

    expectParse("MERGE_DATAPARTS", {MergeDataPartsQuery::create(getExpectedAST())});

    expectParse("INSTALL x", {InstallExtensionQuery::create(getExpectedAST(), "x")});

    expectError("INSTALL",
                "     1 | INSTALL\n"
                "       | ^^^^^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting a name");
}

TEST_F(ParseEdgeCaseTest, parsesExplainOptions) {
    getExpectedAST()->explainRequest().requestDefaults();
    expectParse("EXPLAIN RETURN 1", {query({}, returning({integer(1)}))});

    {
        ExplainRequest& explain = getExpectedAST()->explainRequest();
        explain.requestStage("ast");
        explain.requestStage("nl");

        expectParse("EXPLAIN (ast, nl) RETURN 1", {query({}, returning({integer(1)}))});
    }

    getExpectedAST()->explainRequest().requestAll();
    expectParse("EXPLAIN (all) RETURN 1", {query({}, returning({integer(1)}))});

    expectError("EXPLAIN (bogus) RETURN 1",
                "     1 | EXPLAIN (bogus) RETURN 1\n"
                "       |          ^^^^^\n"
                "-------* Unknown EXPLAIN option 'bogus'. Expected one of ast, vdg, codegen, passes, db, nl, all, or before, after, around followed by a pass name");

    expectError("EXPLAIN (bogus 5",
                "     1 | EXPLAIN (bogus 5\n"
                "       |          ^^^^^\n"
                "-------* Unknown EXPLAIN option 'bogus'. Expected one of ast, vdg, codegen, passes, db, nl, all, or before, after, around followed by a pass name");

    expectError("EXPLAIN (ast 5",
                "     1 | EXPLAIN (ast 5\n"
                "       |              ^\n"
                "-------* syntax error, unexpected DIGIT, expecting CPAREN or OBRACK or COMMA or a name");

    getExpectedAST()->explainRequest().requestPass("before", "x");
    expectParse("EXPLAIN (before x) RETURN 1", {query({}, returning({integer(1)}))});

    {
        ExplainRequest& explain = getExpectedAST()->explainRequest();
        explain.requestPass("before", "x");
        explain.requestPass("before", "y");

        expectParse("EXPLAIN (before [x, y]) RETURN 1", {query({}, returning({integer(1)}))});
    }

    expectError("EXPLAIN (bogus x) RETURN 1",
                "     1 | EXPLAIN (bogus x) RETURN 1\n"
                "       |          ^^^^^\n"
                "-------* Unknown EXPLAIN option 'bogus'. Expected one of ast, vdg, codegen, passes, db, nl, all, or before, after, around followed by a pass name");

    expectError("EXPLAIN (ast x) RETURN 1",
                "     1 | EXPLAIN (ast x) RETURN 1\n"
                "       |          ^^^\n"
                "-------* Unknown EXPLAIN option 'ast'. Expected one of ast, vdg, codegen, passes, db, nl, all, or before, after, around followed by a pass name");

    expectError("EXPLAIN (before [x 5",
                "     1 | EXPLAIN (before [x 5\n"
                "       |                    ^\n"
                "-------* syntax error, unexpected DIGIT, expecting CBRACK or COMMA");

    getExpectedAST()->explainRequest().requestStage("ast");
    expectParse("EXPLAIN (ast) LOAD GRAPH g", {LoadGraphQuery::create(getExpectedAST(), "g")});

    expectError("EXPLAIN LOAD GRAPH g 5",
                "     1 | EXPLAIN LOAD GRAPH g 5\n"
                "       |                      ^\n"
                "-------* syntax error, unexpected DIGIT, expecting PROG_END or SEMI_COLON");

    expectError("EXPLAIN 5",
                "     1 | EXPLAIN 5\n"
                "       |         ^\n"
                "-------* syntax error, unexpected DIGIT");
}

TEST_F(ParseEdgeCaseTest, parsesUnionsNextChainsAndConditionalQueries) {
    expectParse("RETURN EXISTS { { MATCH (a) RETURN a } UNION { MATCH (b) RETURN b } }",
                {query({},
                       returning({exists(unionQuery(query({match({._pattern = pattern({path({node({._symbol = "a"})})})})},
                                                          returning({symbol("a")})),
                                                    {{._query = query({match({._pattern = pattern({path({node({._symbol = "b"})})})})},
                                                                      returning({symbol("b")}))}}))}))});

    expectParse("WITH 1 AS x RETURN x NEXT RETURN x + 1",
                {query({with({._projection = projection({._items = {named(integer(1), "x")}})}),
                        with({._projection = projection({._items = {symbol("x")}}), ._segmentReturn = true})},
                       returning({binary(BinaryOperator::Add, symbol("x"), integer(1))}))});

    expectError("WITH 1 AS x RETURN x + 1 NEXT RETURN 2",
                "     1 | WITH 1 AS x RETURN x + 1 NEXT RETURN 2\n"
                "       | ^^^^^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Expression in a RETURN followed by NEXT must be aliased (use AS)");

    expectParse("RETURN 1 AS a NEXT RETURN a NEXT RETURN a",
                {query({with({._projection = projection({._items = {named(integer(1), "a")}}), ._segmentReturn = true}),
                        with({._projection = projection({._items = {symbol("a")}}), ._segmentReturn = true})},
                       returning({symbol("a")}))});

    expectError("RETURN 1 NEXT",
                "     1 | RETURN 1 NEXT\n"
                "       | ^^^^^^^^\n"
                "-------* Expression in a RETURN followed by NEXT must be aliased (use AS)");

    expectParse("RETURN 1 AS a UNION RETURN 2 AS a NEXT RETURN a",
                {query({nextSegment(callSubquery({._body = unionQuery(query({}, returning({named(integer(1), "a")})),
                                                                      {{._query = query({},
                                                                                        returning({named(integer(2),
                                                                                                         "a")}))}})}))},
                       returning({symbol("a")}))});

    expectParse("{ RETURN 1 AS a } UNION { RETURN 2 AS a }",
                {unionQuery(query({}, returning({named(integer(1), "a")})),
                            {{._query = query({}, returning({named(integer(2), "a")}))}})});

    expectError("{ RETURN 1 }",
                "     1 | { RETURN 1 }\n"
                "       |            ^\n"
                "-------* syntax error, unexpected PROG_END, expecting UNION");

    expectError("{ RETURN 1 } 5",
                "     1 | { RETURN 1 } 5\n"
                "       |              ^\n"
                "-------* syntax error, unexpected DIGIT, expecting UNION");

    expectParse("WHEN true THEN RETURN 1 ELSE RETURN 2",
                {conditional({{._condition = boolean(true), ._query = query({}, returning({integer(1)}))},
                              {._query = query({}, returning({integer(2)}))}})});

    expectParse("WHEN true THEN RETURN 1 AS a NEXT RETURN a",
                {query({nextSegment(callSubquery({._body = conditional({{._condition = boolean(true),
                                                                         ._query = query({},
                                                                                         returning({named(integer(1),
                                                                                                          "a")}))}})}))},
                       returning({symbol("a")}))});

    expectParse("WHEN true THEN { RETURN 1 } WHEN false THEN RETURN 2",
                {conditional({{._condition = boolean(true), ._query = query({}, returning({integer(1)}))},
                              {._condition = boolean(false), ._query = query({}, returning({integer(2)}))}})});

    expectParse("RETURN 1 UNION ALL RETURN 2",
                {unionQuery(query({}, returning({integer(1)})),
                            {{._query = query({}, returning({integer(2)})), ._all = true}})});

    expectError("RETURN 1 UNION 5",
                "     1 | RETURN 1 UNION 5\n"
                "       |                ^\n"
                "-------* syntax error, unexpected DIGIT");
}

TEST_F(ParseEdgeCaseTest, parsesMultiPartQueries) {
    expectParse("MATCH (a) WITH a MATCH (a)-->(b) WITH a, b WHERE b.x > 1 RETURN a, b",
                {query({match({._pattern = pattern({path({node({._symbol = "a"})})})}),
                        with({._projection = projection({._items = {symbol("a")}})}),
                        match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "b"})})})}),
                        with({._projection = projection({._items = {symbol("a"), symbol("b")}}),
                              ._where = where(binary(BinaryOperator::GreaterThan, property({"b", "x"}), integer(1)))})},
                       returning({symbol("a"), symbol("b")}))});

    expectParse("MATCH (a) MATCH (b) OPTIONAL MATCH (a)-->(c) RETURN a, b, c",
                {query({match({._pattern = pattern({path({node({._symbol = "a"})})})}),
                        match({._pattern = pattern({path({node({._symbol = "b"})})})}),
                        match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "c"})})}),
                               ._optional = true})},
                       returning({symbol("a"), symbol("b"), symbol("c")}))});

    expectParse("MATCH (a) UNWIND a.list AS x WITH x ORDER BY x LIMIT 1 RETURN x",
                {query({match({._pattern = pattern({path({node({._symbol = "a"})})})}),
                        unwind(property({"a", "list"}), "x"),
                        with({._projection = projection({._items = {symbol("x")},
                                                         ._orderBy = orderBy({{symbol("x"), OrderByType::ASC}}),
                                                         ._limit = limit(integer(1))})})},
                       returning({symbol("x")}))});

    expectParse("MATCH (a) SET a.x = 1 WITH a MATCH (a)-->(b) DELETE b RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"})})})}),
                        set({setProperty(property({"a", "x"}), integer(1))}),
                        with({._projection = projection({._items = {symbol("a")}})}),
                        match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "b"})})})}),
                        deleteStmt({symbol("b")})},
                       returning({symbol("a")}))});

    expectParse("CREATE (a) WITH a CREATE (a)-[:R]->(b) RETURN a, b",
                {query({createStmt(pattern({path({node({._symbol = "a"})})})),
                        with({._projection = projection({._items = {symbol("a")}})}),
                        createStmt(pattern({path({node({._symbol = "a"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"R"})}),
                                                  node({._symbol = "b"})})}))},
                       returning({symbol("a"), symbol("b")}))});

    expectParse("UNWIND [1, 2] AS x WITH x AS y WITH y * 2 AS z RETURN z",
                {query({unwind(list({integer(1), integer(2)}), "x"),
                        with({._projection = projection({._items = {named(symbol("x"), "y")}})}),
                        with({._projection = projection({._items = {named(binary(BinaryOperator::Mult,
                                                                                 symbol("y"),
                                                                                 integer(2)),
                                                                          "z")}})})},
                       returning({symbol("z")}))});
}

TEST_F(ParseEdgeCaseTest, parsesMatchSegmentsChainedByNext) {
    expectParse("MATCH (a:Person) RETURN a NEXT MATCH (a)-->(b) RETURN a, b",
                {query({match({._pattern = pattern({path({node({._symbol = "a", ._labels = labels({"Person"})})})})}),
                        with({._projection = projection({._items = {symbol("a")}}), ._segmentReturn = true}),
                        match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a"), symbol("b")}))});

    expectParse("MATCH (a) RETURN a NEXT MATCH (a)-->(b) RETURN b NEXT MATCH (b)-->(c) RETURN c",
                {query({match({._pattern = pattern({path({node({._symbol = "a"})})})}),
                        with({._projection = projection({._items = {symbol("a")}}), ._segmentReturn = true}),
                        match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "b"})})})}),
                        with({._projection = projection({._items = {symbol("b")}}), ._segmentReturn = true}),
                        match({._pattern = pattern({path({node({._symbol = "b"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "c"})})})})},
                       returning({symbol("c")}))});

    expectParse("MATCH (a) RETURN a.name AS name NEXT MATCH (b {name: name}) RETURN b",
                {query({match({._pattern = pattern({path({node({._symbol = "a"})})})}),
                        with({._projection = projection({._items = {named(property({"a", "name"}), "name")}}),
                              ._segmentReturn = true}),
                        match({._pattern = pattern({path({node({._symbol = "b",
                                                                ._properties = properties({{"name", symbol("name")}})})})})})},
                       returning({symbol("b")}))});

    expectParse("MATCH (a) RETURN a NEXT OPTIONAL MATCH (a)-->(b) RETURN a, b",
                {query({match({._pattern = pattern({path({node({._symbol = "a"})})})}),
                        with({._projection = projection({._items = {symbol("a")}}), ._segmentReturn = true}),
                        match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "b"})})}),
                               ._optional = true})},
                       returning({symbol("a"), symbol("b")}))});

    expectParse("MATCH (a) RETURN a NEXT MATCH (a)-->(b) WHERE b.x > 1 RETURN count(b) AS n",
                {query({match({._pattern = pattern({path({node({._symbol = "a"})})})}),
                        with({._projection = projection({._items = {symbol("a")}}), ._segmentReturn = true}),
                        match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "b"})})},
                                                   where(binary(BinaryOperator::GreaterThan,
                                                                property({"b", "x"}),
                                                                integer(1))))})},
                       returning({named(invocation({"count"}, {symbol("b")}), "n")}))});

    expectParse("MATCH (a) WITH a MATCH (a)-->(b) RETURN b NEXT MATCH (b)-->(c) WITH c RETURN c",
                {query({match({._pattern = pattern({path({node({._symbol = "a"})})})}),
                        with({._projection = projection({._items = {symbol("a")}})}),
                        match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "b"})})})}),
                        with({._projection = projection({._items = {symbol("b")}}), ._segmentReturn = true}),
                        match({._pattern = pattern({path({node({._symbol = "b"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "c"})})})}),
                        with({._projection = projection({._items = {symbol("c")}})})},
                       returning({symbol("c")}))});

    expectParse("MATCH (a) CREATE (a)-[:R]->(b) RETURN b NEXT MATCH (b)-->(c) RETURN c",
                {query({match({._pattern = pattern({path({node({._symbol = "a"})})})}),
                        createStmt(pattern({path({node({._symbol = "a"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"R"})}),
                                                  node({._symbol = "b"})})})),
                        with({._projection = projection({._items = {symbol("b")}}), ._segmentReturn = true}),
                        match({._pattern = pattern({path({node({._symbol = "b"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "c"})})})})},
                       returning({symbol("c")}))});

    expectParse("MATCH (a) SET a.x = 1 NEXT MATCH (b) RETURN b",
                {query({nextSegment(callSubquery({._body = query({match({._pattern = pattern({path({node({._symbol = "a"})})})}),
                                                                  set({setProperty(property({"a", "x"}), integer(1))})})})),
                        match({._pattern = pattern({path({node({._symbol = "b"})})})})},
                       returning({symbol("b")}))});

    expectParse("MATCH (a) RETURN a NEXT MATCH (b) RETURN b UNION MATCH (c) RETURN c AS b",
                {query({match({._pattern = pattern({path({node({._symbol = "a"})})})}),
                        with({._projection = projection({._items = {symbol("a")}}), ._segmentReturn = true}),
                        nextSegment(callSubquery({._body = unionQuery(query({match({._pattern = pattern({path({node({._symbol = "b"})})})})},
                                                                            returning({symbol("b")})),
                                                                      {{._query = query({match({._pattern = pattern({path({node({._symbol = "c"})})})})},
                                                                                        returning({named(symbol("c"),
                                                                                                         "b")}))}})}))},
                       returning({symbol("b")}))});

    expectError("MATCH (a) RETURN a.x NEXT MATCH (b) RETURN b",
                "     1 | MATCH (a) RETURN a.x NEXT MATCH (b) RETURN b\n"
                "       | ^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Expression in a RETURN followed by NEXT must be aliased (use AS)");
}

TEST_F(ParseEdgeCaseTest, parsesTheJourneyPlannerQueries) {
    expectParse("MATCH (o:STATION {crs: 'OXF'}), (d:STATION {crs: 'PAD'}) "
                "WITH o, d, ['DID', 'RDG', 'PAD'] AS interchanges, 470 AS depart_from, 500 AS depart_before "
                "MATCH leg1=(o)-[:WED]->(firstStop:MODE) -[:NEXT * 0..99]-> (lastStop:MODE)-[:ARRIVE]-(inter:STATION) "
                "WHERE inter.crs IN interchanges AND NOT EXISTS {(lastStop)-[:NEXT|ARRIVE]->{2,20}(d)} AND CASE "
                "WHEN firstStop:FIXED THEN (depart_before > firstStop.start AND depart_from < firstStop.end) "
                "ELSE depart_before >= firstStop.depart >= depart_from END "
                "RETURN firstStop.service AS service, inter.crs AS stop, "
                "interchanges[reduce(found = -1, i IN range(0, size(interchanges) - 1) | CASE "
                "WHEN found = -1 AND interchanges[i] = inter.crs THEN i "
                "ELSE found END) + 1..] AS interchanges, lastStop.nlc AS nlc, firstStop.depart AS departure, "
                "coalesce(lastStop.arrive, depart_from) AS arrival, "
                "coalesce(lastStop.arrive, depart_from) + inter.minInterchange AS departAfter, "
                "coalesce(lastStop.arrive, depart_before) + inter.maxInterchange AS departBefore, "
                "inter = d AS arrived , length(leg1)",
                {query({match({._pattern = pattern({path({node({._symbol = "o",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("OXF")}})})}),
                                                    path({node({._symbol = "d",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("PAD")}})})})})}),
                        with({._projection = projection({._items = {symbol("o"),
                                                                    symbol("d"),
                                                                    named(list({string("DID"),
                                                                                string("RDG"),
                                                                                string("PAD")}),
                                                                          "interchanges"),
                                                                    named(integer(470), "depart_from"),
                                                                    named(integer(500), "depart_before")}})}),
                        match({._pattern = pattern({namedPath("leg1",
                                                              {node({._symbol = "o"}),
                                                               edge({._direction = EdgePattern::Direction::Forward,
                                                                     ._labels = labels({"WED"})}),
                                                               node({._symbol = "firstStop", ._labels = labels({"MODE"})}),
                                                               walk({._direction = EdgePattern::Direction::Forward,
                                                                     ._quantifier = quantifier(0, 99),
                                                                     ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                     ._labels = labels({"NEXT"})})}}),
                                                               node({._symbol = "lastStop", ._labels = labels({"MODE"})}),
                                                               edge({._labels = labels({"ARRIVE"})}),
                                                               node({._symbol = "inter",
                                                                     ._labels = labels({"STATION"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::In,
                                                                         property({"inter", "crs"}),
                                                                         symbol("interchanges")),
                                                                  unary(UnaryOperator::Not,
                                                                        exists(query({match({._pattern = pattern({path({node({._symbol = "lastStop"}),
                                                                                                                        walk({._direction = EdgePattern::Direction::Forward,
                                                                                                                              ._quantifier = quantifier(2,
                                                                                                                                                        20),
                                                                                                                              ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                              ._labels = labelOr({labels({"NEXT"}),
                                                                                                                                                                  labels({"ARRIVE"})})})}}),
                                                                                                                        node({._symbol = "d"})})})})}))),
                                                                  caseExpr(nullptr,
                                                                           {{._tests = {{._value = entityType("firstStop",
                                                                                                              labels({"FIXED"}))}},
                                                                             ._then = parenthesized(logical(LogicalOperator::And,
                                                                                                            {binary(BinaryOperator::GreaterThan,
                                                                                                                    symbol("depart_before"),
                                                                                                                    property({"firstStop", "start"})),
                                                                                                             binary(BinaryOperator::LessThan,
                                                                                                                    symbol("depart_from"),
                                                                                                                    property({"firstStop", "end"}))}))}},
                                                                           comparisonChain(symbol("depart_before"),
                                                                                           {{BinaryOperator::GreaterThanOrEqual, property({"firstStop", "depart"})},
                                                                                            {BinaryOperator::GreaterThanOrEqual, symbol("depart_from")}}))})))})},
                       returning({named(property({"firstStop", "service"}), "service"),
                                  named(property({"inter", "crs"}), "stop"),
                                  named(slice(symbol("interchanges"),
                                              binary(BinaryOperator::Add,
                                                     reduce("found",
                                                            integer(-1),
                                                            "i",
                                                            invocation({"range"},
                                                                       {integer(0),
                                                                        binary(BinaryOperator::Sub,
                                                                               invocation({"size"},
                                                                                          {symbol("interchanges")}),
                                                                               integer(1))}),
                                                            caseExpr(nullptr,
                                                                     {{._tests = {{._value = logical(LogicalOperator::And,
                                                                                                     {binary(BinaryOperator::Equal,
                                                                                                             symbol("found"),
                                                                                                             integer(-1)),
                                                                                                      binary(BinaryOperator::Equal,
                                                                                                             index(symbol("interchanges"),
                                                                                                                   symbol("i")),
                                                                                                             property({"inter", "crs"}))})}},
                                                                       ._then = symbol("i")}},
                                                                     symbol("found"))),
                                                     integer(1)),
                                              nullptr),
                                        "interchanges"),
                                  named(property({"lastStop", "nlc"}), "nlc"),
                                  named(property({"firstStop", "depart"}), "departure"),
                                  named(invocation({"coalesce"}, {property({"lastStop", "arrive"}), symbol("depart_from")}),
                                        "arrival"),
                                  named(binary(BinaryOperator::Add,
                                               invocation({"coalesce"},
                                                          {property({"lastStop", "arrive"}), symbol("depart_from")}),
                                               property({"inter", "minInterchange"})),
                                        "departAfter"),
                                  named(binary(BinaryOperator::Add,
                                               invocation({"coalesce"},
                                                          {property({"lastStop", "arrive"}), symbol("depart_before")}),
                                               property({"inter", "maxInterchange"})),
                                        "departBefore"),
                                  named(binary(BinaryOperator::Equal, symbol("inter"), symbol("d")), "arrived"),
                                  invocation({"length"}, {symbol("leg1")})}))});

    expectParse("MATCH (o:STATION {crs: 'OXF'}), (d:STATION {crs: 'PAD'}) "
                "WITH o, d, ['DID', 'RDG', 'PAD'] AS interchanges, 470 AS depart_from, 500 AS depart_before "
                "MATCH leg1=(o)-[:WED]->(firstStop:MODE) -[:NEXT * 0..99]-> (lastStop:MODE)-[:ARRIVE]-(inter:STATION) "
                "WHERE inter.crs IN interchanges AND NOT EXISTS {(lastStop)-[:NEXT|ARRIVE]->{2,20}(d)} AND CASE "
                "WHEN firstStop:FIXED THEN (depart_before > firstStop.start AND depart_from < firstStop.end) "
                "ELSE depart_before >= firstStop.depart >= depart_from END "
                "RETURN o, d, inter AS prevCalling, leg1 AS path, firstStop.depart AS departure ORDER BY departure "
                "NEXT "
                "RETURN o.crs, d.crs, prevCalling.crs, [n IN nodes(path) | coalesce(n.crs, n.service)], departure",
                {query({match({._pattern = pattern({path({node({._symbol = "o",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("OXF")}})})}),
                                                    path({node({._symbol = "d",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("PAD")}})})})})}),
                        with({._projection = projection({._items = {symbol("o"),
                                                                    symbol("d"),
                                                                    named(list({string("DID"),
                                                                                string("RDG"),
                                                                                string("PAD")}),
                                                                          "interchanges"),
                                                                    named(integer(470), "depart_from"),
                                                                    named(integer(500), "depart_before")}})}),
                        match({._pattern = pattern({namedPath("leg1",
                                                              {node({._symbol = "o"}),
                                                               edge({._direction = EdgePattern::Direction::Forward,
                                                                     ._labels = labels({"WED"})}),
                                                               node({._symbol = "firstStop", ._labels = labels({"MODE"})}),
                                                               walk({._direction = EdgePattern::Direction::Forward,
                                                                     ._quantifier = quantifier(0, 99),
                                                                     ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                     ._labels = labels({"NEXT"})})}}),
                                                               node({._symbol = "lastStop", ._labels = labels({"MODE"})}),
                                                               edge({._labels = labels({"ARRIVE"})}),
                                                               node({._symbol = "inter",
                                                                     ._labels = labels({"STATION"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::In,
                                                                         property({"inter", "crs"}),
                                                                         symbol("interchanges")),
                                                                  unary(UnaryOperator::Not,
                                                                        exists(query({match({._pattern = pattern({path({node({._symbol = "lastStop"}),
                                                                                                                        walk({._direction = EdgePattern::Direction::Forward,
                                                                                                                              ._quantifier = quantifier(2,
                                                                                                                                                        20),
                                                                                                                              ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                              ._labels = labelOr({labels({"NEXT"}),
                                                                                                                                                                  labels({"ARRIVE"})})})}}),
                                                                                                                        node({._symbol = "d"})})})})}))),
                                                                  caseExpr(nullptr,
                                                                           {{._tests = {{._value = entityType("firstStop",
                                                                                                              labels({"FIXED"}))}},
                                                                             ._then = parenthesized(logical(LogicalOperator::And,
                                                                                                            {binary(BinaryOperator::GreaterThan,
                                                                                                                    symbol("depart_before"),
                                                                                                                    property({"firstStop", "start"})),
                                                                                                             binary(BinaryOperator::LessThan,
                                                                                                                    symbol("depart_from"),
                                                                                                                    property({"firstStop", "end"}))}))}},
                                                                           comparisonChain(symbol("depart_before"),
                                                                                           {{BinaryOperator::GreaterThanOrEqual, property({"firstStop", "depart"})},
                                                                                            {BinaryOperator::GreaterThanOrEqual, symbol("depart_from")}}))})))}),
                        with({._projection = projection({._items = {symbol("o"),
                                                                    symbol("d"),
                                                                    named(symbol("inter"), "prevCalling"),
                                                                    named(symbol("leg1"), "path"),
                                                                    named(property({"firstStop", "depart"}), "departure")},
                                                         ._orderBy = orderBy({{symbol("departure"), OrderByType::ASC}})}),
                              ._segmentReturn = true})},
                       returning({property({"o", "crs"}),
                                  property({"d", "crs"}),
                                  property({"prevCalling", "crs"}),
                                  listComprehension("n",
                                                    invocation({"nodes"}, {symbol("path")}),
                                                    nullptr,
                                                    invocation({"coalesce"},
                                                               {property({"n", "crs"}), property({"n", "service"})})),
                                  symbol("departure")}))});

    expectParse("MATCH (d:STATION {crs: 'PAD'}) "
                "MATCH (l:MODE) WHERE NOT EXISTS {(l)-[:NEXT|ARRIVE]->{2,20}(d)} "
                "RETURN count(l)",
                {query({match({._pattern = pattern({path({node({._symbol = "d",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("PAD")}})})})})}),
                        match({._pattern = pattern({path({node({._symbol = "l", ._labels = labels({"MODE"})})})},
                                                   where(unary(UnaryOperator::Not,
                                                               exists(query({match({._pattern = pattern({path({node({._symbol = "l"}),
                                                                                                               walk({._direction = EdgePattern::Direction::Forward,
                                                                                                                     ._quantifier = quantifier(2,
                                                                                                                                               20),
                                                                                                                     ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                     ._labels = labelOr({labels({"NEXT"}),
                                                                                                                                                         labels({"ARRIVE"})})})}}),
                                                                                                               node({._symbol = "d"})})})})})))))})},
                       returning({invocation({"count"}, {symbol("l")})}))});

    expectParse("MATCH (d:STATION {crs: 'PAD'}) "
                "MATCH (l:MODE) WHERE NOT EXISTS {(l)-[:NEXT|ARRIVE*2..20]->(d)} "
                "RETURN count(l)",
                {query({match({._pattern = pattern({path({node({._symbol = "d",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("PAD")}})})})})}),
                        match({._pattern = pattern({path({node({._symbol = "l", ._labels = labels({"MODE"})})})},
                                                   where(unary(UnaryOperator::Not,
                                                               exists(query({match({._pattern = pattern({path({node({._symbol = "l"}),
                                                                                                               walk({._direction = EdgePattern::Direction::Forward,
                                                                                                                     ._quantifier = quantifier(2,
                                                                                                                                               20),
                                                                                                                     ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                     ._labels = labelOr({labels({"NEXT"}),
                                                                                                                                                         labels({"ARRIVE"})})})}}),
                                                                                                               node({._symbol = "d"})})})})})))))})},
                       returning({invocation({"count"}, {symbol("l")})}))});

    expectParse("WITH [{service: 'a', nlc: 'A', departure: 482, arrival: 497}, {service: 'b', nlc: 'A', departure: 490, arrival: 502}] AS OTTlist "
                "RETURN [leg IN reduce(retain = [OTTlist[0]], i IN range(1, size(OTTlist) - 1) | CASE "
                "WHEN OTTlist[i].nlc = retain[-1].nlc THEN CASE WHEN (OTTlist[i].departure <= retain[-1].departure "
                "AND OTTlist[i].arrival >= retain[-1].arrival) THEN retain "
                "WHEN (OTTlist[i].departure = retain[-1].departure "
                "AND OTTlist[i].arrival < retain[-1].arrival) THEN retain[..-1] + OTTlist[i] "
                "ELSE retain + OTTlist[i] END ELSE retain + OTTlist[i] END) | leg.service]",
                {query({with({._projection = projection({._items = {named(list({map({{"service", string("a")},
                                                                                     {"nlc", string("A")},
                                                                                     {"departure", integer(482)},
                                                                                     {"arrival", integer(497)}}),
                                                                                map({{"service", string("b")},
                                                                                     {"nlc", string("A")},
                                                                                     {"departure", integer(490)},
                                                                                     {"arrival", integer(502)}})}),
                                                                          "OTTlist")}})})},
                       returning({listComprehension("leg",
                                                    reduce("retain",
                                                           list({index(symbol("OTTlist"), integer(0))}),
                                                           "i",
                                                           invocation({"range"},
                                                                      {integer(1),
                                                                       binary(BinaryOperator::Sub,
                                                                              invocation({"size"}, {symbol("OTTlist")}),
                                                                              integer(1))}),
                                                           caseExpr(nullptr,
                                                                    {{._tests = {{._value = binary(BinaryOperator::Equal,
                                                                                                   lookup(index(symbol("OTTlist"),
                                                                                                                symbol("i")),
                                                                                                          "nlc"),
                                                                                                   lookup(index(symbol("retain"),
                                                                                                                integer(-1)),
                                                                                                          "nlc"))}},
                                                                      ._then = caseExpr(nullptr,
                                                                                        {{._tests = {{._value = parenthesized(logical(LogicalOperator::And,
                                                                                                                                      {binary(BinaryOperator::LessThanOrEqual,
                                                                                                                                              lookup(index(symbol("OTTlist"),
                                                                                                                                                           symbol("i")),
                                                                                                                                                     "departure"),
                                                                                                                                              lookup(index(symbol("retain"),
                                                                                                                                                           integer(-1)),
                                                                                                                                                     "departure")),
                                                                                                                                       binary(BinaryOperator::GreaterThanOrEqual,
                                                                                                                                              lookup(index(symbol("OTTlist"),
                                                                                                                                                           symbol("i")),
                                                                                                                                                     "arrival"),
                                                                                                                                              lookup(index(symbol("retain"),
                                                                                                                                                           integer(-1)),
                                                                                                                                                     "arrival"))}))}},
                                                                                          ._then = symbol("retain")},
                                                                                         {._tests = {{._value = parenthesized(logical(LogicalOperator::And,
                                                                                                                                      {binary(BinaryOperator::Equal,
                                                                                                                                              lookup(index(symbol("OTTlist"),
                                                                                                                                                           symbol("i")),
                                                                                                                                                     "departure"),
                                                                                                                                              lookup(index(symbol("retain"),
                                                                                                                                                           integer(-1)),
                                                                                                                                                     "departure")),
                                                                                                                                       binary(BinaryOperator::LessThan,
                                                                                                                                              lookup(index(symbol("OTTlist"),
                                                                                                                                                           symbol("i")),
                                                                                                                                                     "arrival"),
                                                                                                                                              lookup(index(symbol("retain"),
                                                                                                                                                           integer(-1)),
                                                                                                                                                     "arrival"))}))}},
                                                                                          ._then = binary(BinaryOperator::Add,
                                                                                                          slice(symbol("retain"),
                                                                                                                nullptr,
                                                                                                                integer(-1)),
                                                                                                          index(symbol("OTTlist"),
                                                                                                                symbol("i")))}},
                                                                                        binary(BinaryOperator::Add,
                                                                                               symbol("retain"),
                                                                                               index(symbol("OTTlist"),
                                                                                                     symbol("i"))))}},
                                                                    binary(BinaryOperator::Add,
                                                                           symbol("retain"),
                                                                           index(symbol("OTTlist"), symbol("i"))))),
                                                    nullptr,
                                                    property({"leg", "service"}))}))});

    expectParse("MATCH (o:STATION {crs: 'OXF'}), (d:STATION {crs: 'PAD'}) "
                "WITH o, d, ['DID', 'RDG', 'PAD'] AS interchanges, 470 AS depart_from, 500 AS depart_before "
                "MATCH leg1=(o)-[:WED]->(firstStop:MODE) -[:NEXT * 0..99]-> (lastStop:MODE)-[:ARRIVE]-(inter:STATION) "
                "WHERE inter.crs IN interchanges AND NOT EXISTS {(lastStop)-[:NEXT|ARRIVE]->{2,20}(d)} AND CASE "
                "WHEN firstStop:FIXED THEN (depart_before > firstStop.start AND depart_from < firstStop.end) "
                "ELSE depart_before >= firstStop.depart >= depart_from END "
                "WITH firstStop.service AS service, inter.crs AS stop, "
                "interchanges[reduce(found = -1, i IN range(0, size(interchanges) - 1) | CASE "
                "WHEN found = -1 AND interchanges[i] = inter.crs THEN i "
                "ELSE found END) + 1..] AS interchanges, lastStop.nlc AS nlc, firstStop.depart AS departure, "
                "coalesce(lastStop.arrive, depart_from) AS arrival, "
                "coalesce(lastStop.arrive, depart_from) + inter.minInterchange AS departAfter, "
                "coalesce(lastStop.arrive, depart_before) + inter.maxInterchange AS departBefore, inter = d AS arrived "
                "WITH service, stop, interchanges, nlc, departure, arrival, departAfter, departBefore, arrived "
                "ORDER BY nlc DESC, arrival, departure DESC "
                "WITH collect([service, stop, interchanges, nlc, departure, arrival, departAfter, departBefore, arrived]) AS rows "
                "WITH [r IN rows | {service: r[0], stop: r[1], interchanges: r[2], nlc: r[3], departure: r[4], arrival: r[5], departAfter: r[6], departBefore: r[7], arrived: r[8]}] AS OTTlist "
                "WITH reduce(retain = [OTTlist[0]], i IN range(1, size(OTTlist) - 1) | CASE "
                "WHEN OTTlist[i].nlc = retain[-1].nlc THEN CASE WHEN (OTTlist[i].departure <= retain[-1].departure "
                "AND OTTlist[i].arrival >= retain[-1].arrival) THEN retain "
                "WHEN (OTTlist[i].departure = retain[-1].departure "
                "AND OTTlist[i].arrival < retain[-1].arrival) THEN retain[..-1] + OTTlist[i] "
                "ELSE retain + OTTlist[i] END ELSE retain + OTTlist[i] END) AS OTTlist "
                "RETURN [leg IN OTTlist | leg.service]",
                {query({match({._pattern = pattern({path({node({._symbol = "o",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("OXF")}})})}),
                                                    path({node({._symbol = "d",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("PAD")}})})})})}),
                        with({._projection = projection({._items = {symbol("o"),
                                                                    symbol("d"),
                                                                    named(list({string("DID"),
                                                                                string("RDG"),
                                                                                string("PAD")}),
                                                                          "interchanges"),
                                                                    named(integer(470), "depart_from"),
                                                                    named(integer(500), "depart_before")}})}),
                        match({._pattern = pattern({namedPath("leg1",
                                                              {node({._symbol = "o"}),
                                                               edge({._direction = EdgePattern::Direction::Forward,
                                                                     ._labels = labels({"WED"})}),
                                                               node({._symbol = "firstStop", ._labels = labels({"MODE"})}),
                                                               walk({._direction = EdgePattern::Direction::Forward,
                                                                     ._quantifier = quantifier(0, 99),
                                                                     ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                     ._labels = labels({"NEXT"})})}}),
                                                               node({._symbol = "lastStop", ._labels = labels({"MODE"})}),
                                                               edge({._labels = labels({"ARRIVE"})}),
                                                               node({._symbol = "inter",
                                                                     ._labels = labels({"STATION"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::In,
                                                                         property({"inter", "crs"}),
                                                                         symbol("interchanges")),
                                                                  unary(UnaryOperator::Not,
                                                                        exists(query({match({._pattern = pattern({path({node({._symbol = "lastStop"}),
                                                                                                                        walk({._direction = EdgePattern::Direction::Forward,
                                                                                                                              ._quantifier = quantifier(2,
                                                                                                                                                        20),
                                                                                                                              ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                              ._labels = labelOr({labels({"NEXT"}),
                                                                                                                                                                  labels({"ARRIVE"})})})}}),
                                                                                                                        node({._symbol = "d"})})})})}))),
                                                                  caseExpr(nullptr,
                                                                           {{._tests = {{._value = entityType("firstStop",
                                                                                                              labels({"FIXED"}))}},
                                                                             ._then = parenthesized(logical(LogicalOperator::And,
                                                                                                            {binary(BinaryOperator::GreaterThan,
                                                                                                                    symbol("depart_before"),
                                                                                                                    property({"firstStop", "start"})),
                                                                                                             binary(BinaryOperator::LessThan,
                                                                                                                    symbol("depart_from"),
                                                                                                                    property({"firstStop", "end"}))}))}},
                                                                           comparisonChain(symbol("depart_before"),
                                                                                           {{BinaryOperator::GreaterThanOrEqual, property({"firstStop", "depart"})},
                                                                                            {BinaryOperator::GreaterThanOrEqual, symbol("depart_from")}}))})))}),
                        with({._projection = projection({._items = {named(property({"firstStop", "service"}), "service"),
                                                                    named(property({"inter", "crs"}), "stop"),
                                                                    named(slice(symbol("interchanges"),
                                                                                binary(BinaryOperator::Add,
                                                                                       reduce("found",
                                                                                              integer(-1),
                                                                                              "i",
                                                                                              invocation({"range"},
                                                                                                         {integer(0),
                                                                                                          binary(BinaryOperator::Sub,
                                                                                                                 invocation({"size"},
                                                                                                                            {symbol("interchanges")}),
                                                                                                                 integer(1))}),
                                                                                              caseExpr(nullptr,
                                                                                                       {{._tests = {{._value = logical(LogicalOperator::And,
                                                                                                                                       {binary(BinaryOperator::Equal,
                                                                                                                                               symbol("found"),
                                                                                                                                               integer(-1)),
                                                                                                                                        binary(BinaryOperator::Equal,
                                                                                                                                               index(symbol("interchanges"),
                                                                                                                                                     symbol("i")),
                                                                                                                                               property({"inter", "crs"}))})}},
                                                                                                         ._then = symbol("i")}},
                                                                                                       symbol("found"))),
                                                                                       integer(1)),
                                                                                nullptr),
                                                                          "interchanges"),
                                                                    named(property({"lastStop", "nlc"}), "nlc"),
                                                                    named(property({"firstStop", "depart"}), "departure"),
                                                                    named(invocation({"coalesce"},
                                                                                     {property({"lastStop", "arrive"}),
                                                                                      symbol("depart_from")}),
                                                                          "arrival"),
                                                                    named(binary(BinaryOperator::Add,
                                                                                 invocation({"coalesce"},
                                                                                            {property({"lastStop", "arrive"}),
                                                                                             symbol("depart_from")}),
                                                                                 property({"inter", "minInterchange"})),
                                                                          "departAfter"),
                                                                    named(binary(BinaryOperator::Add,
                                                                                 invocation({"coalesce"},
                                                                                            {property({"lastStop", "arrive"}),
                                                                                             symbol("depart_before")}),
                                                                                 property({"inter", "maxInterchange"})),
                                                                          "departBefore"),
                                                                    named(binary(BinaryOperator::Equal,
                                                                                 symbol("inter"),
                                                                                 symbol("d")),
                                                                          "arrived")}})}),
                        with({._projection = projection({._items = {symbol("service"),
                                                                    symbol("stop"),
                                                                    symbol("interchanges"),
                                                                    symbol("nlc"),
                                                                    symbol("departure"),
                                                                    symbol("arrival"),
                                                                    symbol("departAfter"),
                                                                    symbol("departBefore"),
                                                                    symbol("arrived")},
                                                         ._orderBy = orderBy({{symbol("nlc"), OrderByType::DESC},
                                                                              {symbol("arrival"), OrderByType::ASC},
                                                                              {symbol("departure"),
                                                                               OrderByType::DESC}})})}),
                        with({._projection = projection({._items = {named(invocation({"collect"},
                                                                                     {list({symbol("service"),
                                                                                            symbol("stop"),
                                                                                            symbol("interchanges"),
                                                                                            symbol("nlc"),
                                                                                            symbol("departure"),
                                                                                            symbol("arrival"),
                                                                                            symbol("departAfter"),
                                                                                            symbol("departBefore"),
                                                                                            symbol("arrived")})}),
                                                                          "rows")}})}),
                        with({._projection = projection({._items = {named(listComprehension("r",
                                                                                            symbol("rows"),
                                                                                            nullptr,
                                                                                            map({{"service", index(symbol("r"),
                                                                                                                   integer(0))},
                                                                                                 {"stop", index(symbol("r"),
                                                                                                                integer(1))},
                                                                                                 {"interchanges", index(symbol("r"),
                                                                                                                        integer(2))},
                                                                                                 {"nlc", index(symbol("r"),
                                                                                                               integer(3))},
                                                                                                 {"departure", index(symbol("r"),
                                                                                                                     integer(4))},
                                                                                                 {"arrival", index(symbol("r"),
                                                                                                                   integer(5))},
                                                                                                 {"departAfter", index(symbol("r"),
                                                                                                                       integer(6))},
                                                                                                 {"departBefore", index(symbol("r"),
                                                                                                                        integer(7))},
                                                                                                 {"arrived", index(symbol("r"),
                                                                                                                   integer(8))}})),
                                                                          "OTTlist")}})}),
                        with({._projection = projection({._items = {named(reduce("retain",
                                                                                 list({index(symbol("OTTlist"),
                                                                                             integer(0))}),
                                                                                 "i",
                                                                                 invocation({"range"},
                                                                                            {integer(1),
                                                                                             binary(BinaryOperator::Sub,
                                                                                                    invocation({"size"},
                                                                                                               {symbol("OTTlist")}),
                                                                                                    integer(1))}),
                                                                                 caseExpr(nullptr,
                                                                                          {{._tests = {{._value = binary(BinaryOperator::Equal,
                                                                                                                         lookup(index(symbol("OTTlist"),
                                                                                                                                      symbol("i")),
                                                                                                                                "nlc"),
                                                                                                                         lookup(index(symbol("retain"),
                                                                                                                                      integer(-1)),
                                                                                                                                "nlc"))}},
                                                                                            ._then = caseExpr(nullptr,
                                                                                                              {{._tests = {{._value = parenthesized(logical(LogicalOperator::And,
                                                                                                                                                            {binary(BinaryOperator::LessThanOrEqual,
                                                                                                                                                                    lookup(index(symbol("OTTlist"),
                                                                                                                                                                                 symbol("i")),
                                                                                                                                                                           "departure"),
                                                                                                                                                                    lookup(index(symbol("retain"),
                                                                                                                                                                                 integer(-1)),
                                                                                                                                                                           "departure")),
                                                                                                                                                             binary(BinaryOperator::GreaterThanOrEqual,
                                                                                                                                                                    lookup(index(symbol("OTTlist"),
                                                                                                                                                                                 symbol("i")),
                                                                                                                                                                           "arrival"),
                                                                                                                                                                    lookup(index(symbol("retain"),
                                                                                                                                                                                 integer(-1)),
                                                                                                                                                                           "arrival"))}))}},
                                                                                                                ._then = symbol("retain")},
                                                                                                               {._tests = {{._value = parenthesized(logical(LogicalOperator::And,
                                                                                                                                                            {binary(BinaryOperator::Equal,
                                                                                                                                                                    lookup(index(symbol("OTTlist"),
                                                                                                                                                                                 symbol("i")),
                                                                                                                                                                           "departure"),
                                                                                                                                                                    lookup(index(symbol("retain"),
                                                                                                                                                                                 integer(-1)),
                                                                                                                                                                           "departure")),
                                                                                                                                                             binary(BinaryOperator::LessThan,
                                                                                                                                                                    lookup(index(symbol("OTTlist"),
                                                                                                                                                                                 symbol("i")),
                                                                                                                                                                           "arrival"),
                                                                                                                                                                    lookup(index(symbol("retain"),
                                                                                                                                                                                 integer(-1)),
                                                                                                                                                                           "arrival"))}))}},
                                                                                                                ._then = binary(BinaryOperator::Add,
                                                                                                                                slice(symbol("retain"),
                                                                                                                                      nullptr,
                                                                                                                                      integer(-1)),
                                                                                                                                index(symbol("OTTlist"),
                                                                                                                                      symbol("i")))}},
                                                                                                              binary(BinaryOperator::Add,
                                                                                                                     symbol("retain"),
                                                                                                                     index(symbol("OTTlist"),
                                                                                                                           symbol("i"))))}},
                                                                                          binary(BinaryOperator::Add,
                                                                                                 symbol("retain"),
                                                                                                 index(symbol("OTTlist"),
                                                                                                       symbol("i"))))),
                                                                          "OTTlist")}})})},
                       returning({listComprehension("leg", symbol("OTTlist"), nullptr, property({"leg", "service"}))}))});

    expectParse("MATCH (:STATION {crs: 'OXF'})-[:WED]->(f:MODE) "
                "WITH f ORDER BY f.depart DESC, f.service "
                "RETURN collect(f.service)",
                {query({match({._pattern = pattern({path({node({._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("OXF")}})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"WED"})}),
                                                          node({._symbol = "f", ._labels = labels({"MODE"})})})})}),
                        with({._projection = projection({._items = {symbol("f")},
                                                         ._orderBy = orderBy({{property({"f", "depart"}),
                                                                               OrderByType::DESC},
                                                                              {property({"f", "service"}),
                                                                               OrderByType::ASC}})})})},
                       returning({invocation({"collect"}, {property({"f", "service"})})}))});

    expectParse("MATCH (:STATION {crs: 'OXF'})-[:WED]->(f:MODE) "
                "WITH f ORDER BY f.depart "
                "WITH collect(f) AS fs "
                "RETURN [x IN fs WHERE x.depart IS NOT NULL | {service: x.service, departure: x.depart}][0..2]",
                {query({match({._pattern = pattern({path({node({._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("OXF")}})}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._labels = labels({"WED"})}),
                                                          node({._symbol = "f", ._labels = labels({"MODE"})})})})}),
                        with({._projection = projection({._items = {symbol("f")},
                                                         ._orderBy = orderBy({{property({"f", "depart"}),
                                                                               OrderByType::ASC}})})}),
                        with({._projection = projection({._items = {named(invocation({"collect"}, {symbol("f")}),
                                                                          "fs")}})})},
                       returning({slice(listComprehension("x",
                                                          symbol("fs"),
                                                          binary(BinaryOperator::IsNotNull,
                                                                 property({"x", "depart"}),
                                                                 null()),
                                                          map({{"service", property({"x", "service"})},
                                                               {"departure", property({"x", "depart"})}})),
                                        integer(0),
                                        integer(2))}))});

    expectParse("WITH [{nlc: 'A', departure: 480, arrival: 500}, {nlc: 'A', departure: 470, arrival: 510}] AS l "
                "RETURN l[-1].nlc, l[..-1], l + {nlc: 'B'}, [l[0]], size(l)",
                {query({with({._projection = projection({._items = {named(list({map({{"nlc", string("A")},
                                                                                     {"departure", integer(480)},
                                                                                     {"arrival", integer(500)}}),
                                                                                map({{"nlc", string("A")},
                                                                                     {"departure", integer(470)},
                                                                                     {"arrival", integer(510)}})}),
                                                                          "l")}})})},
                       returning({lookup(index(symbol("l"), integer(-1)), "nlc"),
                                  slice(symbol("l"), nullptr, integer(-1)),
                                  binary(BinaryOperator::Add, symbol("l"), map({{"nlc", string("B")}})),
                                  list({index(symbol("l"), integer(0))}),
                                  invocation({"size"}, {symbol("l")})}))});

    expectParse("MATCH (o:STATION {crs: 'OXF'}), (d:STATION {crs: 'PAD'}) "
                "WITH o, d, ['DID', 'RDG', 'PAD'] AS interchanges, 470 AS depart_from, 500 AS depart_before "
                "MATCH leg1=(o)-[:WED]->(firstStop:MODE) -[:NEXT * 0..99]-> (lastStop:MODE)-[:ARRIVE]-(inter:STATION) "
                "WHERE inter.crs IN interchanges AND NOT EXISTS {(lastStop)-[:NEXT|ARRIVE]->{2,20}(d)} AND CASE "
                "WHEN firstStop:FIXED THEN (depart_before > firstStop.start AND depart_from < firstStop.end) "
                "ELSE depart_before >= firstStop.depart >= depart_from END "
                "WITH {service: firstStop.service, path: leg1} AS leg "
                "RETURN leg.service, length(leg.path)",
                {query({match({._pattern = pattern({path({node({._symbol = "o",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("OXF")}})})}),
                                                    path({node({._symbol = "d",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("PAD")}})})})})}),
                        with({._projection = projection({._items = {symbol("o"),
                                                                    symbol("d"),
                                                                    named(list({string("DID"),
                                                                                string("RDG"),
                                                                                string("PAD")}),
                                                                          "interchanges"),
                                                                    named(integer(470), "depart_from"),
                                                                    named(integer(500), "depart_before")}})}),
                        match({._pattern = pattern({namedPath("leg1",
                                                              {node({._symbol = "o"}),
                                                               edge({._direction = EdgePattern::Direction::Forward,
                                                                     ._labels = labels({"WED"})}),
                                                               node({._symbol = "firstStop", ._labels = labels({"MODE"})}),
                                                               walk({._direction = EdgePattern::Direction::Forward,
                                                                     ._quantifier = quantifier(0, 99),
                                                                     ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                     ._labels = labels({"NEXT"})})}}),
                                                               node({._symbol = "lastStop", ._labels = labels({"MODE"})}),
                                                               edge({._labels = labels({"ARRIVE"})}),
                                                               node({._symbol = "inter",
                                                                     ._labels = labels({"STATION"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::In,
                                                                         property({"inter", "crs"}),
                                                                         symbol("interchanges")),
                                                                  unary(UnaryOperator::Not,
                                                                        exists(query({match({._pattern = pattern({path({node({._symbol = "lastStop"}),
                                                                                                                        walk({._direction = EdgePattern::Direction::Forward,
                                                                                                                              ._quantifier = quantifier(2,
                                                                                                                                                        20),
                                                                                                                              ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                              ._labels = labelOr({labels({"NEXT"}),
                                                                                                                                                                  labels({"ARRIVE"})})})}}),
                                                                                                                        node({._symbol = "d"})})})})}))),
                                                                  caseExpr(nullptr,
                                                                           {{._tests = {{._value = entityType("firstStop",
                                                                                                              labels({"FIXED"}))}},
                                                                             ._then = parenthesized(logical(LogicalOperator::And,
                                                                                                            {binary(BinaryOperator::GreaterThan,
                                                                                                                    symbol("depart_before"),
                                                                                                                    property({"firstStop", "start"})),
                                                                                                             binary(BinaryOperator::LessThan,
                                                                                                                    symbol("depart_from"),
                                                                                                                    property({"firstStop", "end"}))}))}},
                                                                           comparisonChain(symbol("depart_before"),
                                                                                           {{BinaryOperator::GreaterThanOrEqual, property({"firstStop", "depart"})},
                                                                                            {BinaryOperator::GreaterThanOrEqual, symbol("depart_from")}}))})))}),
                        with({._projection = projection({._items = {named(map({{"service", property({"firstStop", "service"})},
                                                                               {"path", symbol("leg1")}}),
                                                                          "leg")}})})},
                       returning({property({"leg", "service"}), invocation({"length"}, {property({"leg", "path"})})}))});

    expectParse("MATCH (o:STATION {crs: 'OXF'}), (d:STATION {crs: 'PAD'}) "
                "WITH o, d, ['DID', 'RDG', 'PAD'] AS interchanges, 470 AS depart_from, 500 AS depart_before "
                "MATCH leg1=(o)-[:WED]->(firstStop:MODE) -[:NEXT * 0..99]-> (lastStop:MODE)-[:ARRIVE]-(inter:STATION) "
                "WHERE inter.crs IN interchanges AND NOT EXISTS {(lastStop)-[:NEXT|ARRIVE]->{2,20}(d)} AND CASE "
                "WHEN firstStop:FIXED THEN (depart_before > firstStop.start AND depart_from < firstStop.end) "
                "ELSE depart_before >= firstStop.depart >= depart_from END "
                "WITH firstStop.service AS service, inter.crs AS stop, "
                "interchanges[reduce(found = -1, i IN range(0, size(interchanges) - 1) | CASE "
                "WHEN found = -1 AND interchanges[i] = inter.crs THEN i "
                "ELSE found END) + 1..] AS interchanges, lastStop.nlc AS nlc, firstStop.depart AS departure, "
                "coalesce(lastStop.arrive, depart_from) AS arrival, "
                "coalesce(lastStop.arrive, depart_from) + inter.minInterchange AS departAfter, "
                "coalesce(lastStop.arrive, depart_before) + inter.maxInterchange AS departBefore, inter = d AS arrived "
                "WITH service, stop, interchanges, nlc, departure, arrival, departAfter, departBefore, arrived "
                "ORDER BY nlc DESC, arrival, departure DESC "
                "WITH collect([service, stop, interchanges, nlc, departure, arrival, departAfter, departBefore, arrived]) AS rows "
                "WITH [r IN rows | {service: r[0], stop: r[1], interchanges: r[2], nlc: r[3], departure: r[4], arrival: r[5], departAfter: r[6], departBefore: r[7], arrived: r[8]}] AS OTTlist "
                "WITH reduce(retain = [OTTlist[0]], i IN range(1, size(OTTlist) - 1) | CASE "
                "WHEN OTTlist[i].nlc = retain[-1].nlc THEN CASE WHEN (OTTlist[i].departure <= retain[-1].departure "
                "AND OTTlist[i].arrival >= retain[-1].arrival) THEN retain "
                "WHEN (OTTlist[i].departure = retain[-1].departure "
                "AND OTTlist[i].arrival < retain[-1].arrival) THEN retain[..-1] + OTTlist[i] "
                "ELSE retain + OTTlist[i] END ELSE retain + OTTlist[i] END) AS OTTlist "
                "UNWIND OTTlist AS leg "
                "WITH leg WHERE NOT leg.arrived "
                "MATCH (o:STATION {crs: leg.stop}), (d:STATION {crs: 'PAD'}) "
                "WITH leg, o, d, leg.interchanges AS interchanges, leg.departAfter AS depart_from, "
                "leg.departBefore AS depart_before "
                "MATCH leg1=(o)-[:WED]->(firstStop:MODE) -[:NEXT * 0..99]-> (lastStop:MODE)-[:ARRIVE]-(inter:STATION) "
                "WHERE inter.crs IN interchanges AND NOT EXISTS {(lastStop)-[:NEXT|ARRIVE]->{2,20}(d)} AND CASE "
                "WHEN firstStop:FIXED THEN (depart_before > firstStop.start AND depart_from < firstStop.end) "
                "ELSE depart_before >= firstStop.depart >= depart_from END AND inter = d "
                "RETURN leg.service AS first, firstStop.service AS second, lastStop.arrive AS arrival "
                "ORDER BY arrival, first, second",
                {query({match({._pattern = pattern({path({node({._symbol = "o",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("OXF")}})})}),
                                                    path({node({._symbol = "d",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("PAD")}})})})})}),
                        with({._projection = projection({._items = {symbol("o"),
                                                                    symbol("d"),
                                                                    named(list({string("DID"),
                                                                                string("RDG"),
                                                                                string("PAD")}),
                                                                          "interchanges"),
                                                                    named(integer(470), "depart_from"),
                                                                    named(integer(500), "depart_before")}})}),
                        match({._pattern = pattern({namedPath("leg1",
                                                              {node({._symbol = "o"}),
                                                               edge({._direction = EdgePattern::Direction::Forward,
                                                                     ._labels = labels({"WED"})}),
                                                               node({._symbol = "firstStop", ._labels = labels({"MODE"})}),
                                                               walk({._direction = EdgePattern::Direction::Forward,
                                                                     ._quantifier = quantifier(0, 99),
                                                                     ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                     ._labels = labels({"NEXT"})})}}),
                                                               node({._symbol = "lastStop", ._labels = labels({"MODE"})}),
                                                               edge({._labels = labels({"ARRIVE"})}),
                                                               node({._symbol = "inter",
                                                                     ._labels = labels({"STATION"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::In,
                                                                         property({"inter", "crs"}),
                                                                         symbol("interchanges")),
                                                                  unary(UnaryOperator::Not,
                                                                        exists(query({match({._pattern = pattern({path({node({._symbol = "lastStop"}),
                                                                                                                        walk({._direction = EdgePattern::Direction::Forward,
                                                                                                                              ._quantifier = quantifier(2,
                                                                                                                                                        20),
                                                                                                                              ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                              ._labels = labelOr({labels({"NEXT"}),
                                                                                                                                                                  labels({"ARRIVE"})})})}}),
                                                                                                                        node({._symbol = "d"})})})})}))),
                                                                  caseExpr(nullptr,
                                                                           {{._tests = {{._value = entityType("firstStop",
                                                                                                              labels({"FIXED"}))}},
                                                                             ._then = parenthesized(logical(LogicalOperator::And,
                                                                                                            {binary(BinaryOperator::GreaterThan,
                                                                                                                    symbol("depart_before"),
                                                                                                                    property({"firstStop", "start"})),
                                                                                                             binary(BinaryOperator::LessThan,
                                                                                                                    symbol("depart_from"),
                                                                                                                    property({"firstStop", "end"}))}))}},
                                                                           comparisonChain(symbol("depart_before"),
                                                                                           {{BinaryOperator::GreaterThanOrEqual, property({"firstStop", "depart"})},
                                                                                            {BinaryOperator::GreaterThanOrEqual, symbol("depart_from")}}))})))}),
                        with({._projection = projection({._items = {named(property({"firstStop", "service"}), "service"),
                                                                    named(property({"inter", "crs"}), "stop"),
                                                                    named(slice(symbol("interchanges"),
                                                                                binary(BinaryOperator::Add,
                                                                                       reduce("found",
                                                                                              integer(-1),
                                                                                              "i",
                                                                                              invocation({"range"},
                                                                                                         {integer(0),
                                                                                                          binary(BinaryOperator::Sub,
                                                                                                                 invocation({"size"},
                                                                                                                            {symbol("interchanges")}),
                                                                                                                 integer(1))}),
                                                                                              caseExpr(nullptr,
                                                                                                       {{._tests = {{._value = logical(LogicalOperator::And,
                                                                                                                                       {binary(BinaryOperator::Equal,
                                                                                                                                               symbol("found"),
                                                                                                                                               integer(-1)),
                                                                                                                                        binary(BinaryOperator::Equal,
                                                                                                                                               index(symbol("interchanges"),
                                                                                                                                                     symbol("i")),
                                                                                                                                               property({"inter", "crs"}))})}},
                                                                                                         ._then = symbol("i")}},
                                                                                                       symbol("found"))),
                                                                                       integer(1)),
                                                                                nullptr),
                                                                          "interchanges"),
                                                                    named(property({"lastStop", "nlc"}), "nlc"),
                                                                    named(property({"firstStop", "depart"}), "departure"),
                                                                    named(invocation({"coalesce"},
                                                                                     {property({"lastStop", "arrive"}),
                                                                                      symbol("depart_from")}),
                                                                          "arrival"),
                                                                    named(binary(BinaryOperator::Add,
                                                                                 invocation({"coalesce"},
                                                                                            {property({"lastStop", "arrive"}),
                                                                                             symbol("depart_from")}),
                                                                                 property({"inter", "minInterchange"})),
                                                                          "departAfter"),
                                                                    named(binary(BinaryOperator::Add,
                                                                                 invocation({"coalesce"},
                                                                                            {property({"lastStop", "arrive"}),
                                                                                             symbol("depart_before")}),
                                                                                 property({"inter", "maxInterchange"})),
                                                                          "departBefore"),
                                                                    named(binary(BinaryOperator::Equal,
                                                                                 symbol("inter"),
                                                                                 symbol("d")),
                                                                          "arrived")}})}),
                        with({._projection = projection({._items = {symbol("service"),
                                                                    symbol("stop"),
                                                                    symbol("interchanges"),
                                                                    symbol("nlc"),
                                                                    symbol("departure"),
                                                                    symbol("arrival"),
                                                                    symbol("departAfter"),
                                                                    symbol("departBefore"),
                                                                    symbol("arrived")},
                                                         ._orderBy = orderBy({{symbol("nlc"), OrderByType::DESC},
                                                                              {symbol("arrival"), OrderByType::ASC},
                                                                              {symbol("departure"),
                                                                               OrderByType::DESC}})})}),
                        with({._projection = projection({._items = {named(invocation({"collect"},
                                                                                     {list({symbol("service"),
                                                                                            symbol("stop"),
                                                                                            symbol("interchanges"),
                                                                                            symbol("nlc"),
                                                                                            symbol("departure"),
                                                                                            symbol("arrival"),
                                                                                            symbol("departAfter"),
                                                                                            symbol("departBefore"),
                                                                                            symbol("arrived")})}),
                                                                          "rows")}})}),
                        with({._projection = projection({._items = {named(listComprehension("r",
                                                                                            symbol("rows"),
                                                                                            nullptr,
                                                                                            map({{"service", index(symbol("r"),
                                                                                                                   integer(0))},
                                                                                                 {"stop", index(symbol("r"),
                                                                                                                integer(1))},
                                                                                                 {"interchanges", index(symbol("r"),
                                                                                                                        integer(2))},
                                                                                                 {"nlc", index(symbol("r"),
                                                                                                               integer(3))},
                                                                                                 {"departure", index(symbol("r"),
                                                                                                                     integer(4))},
                                                                                                 {"arrival", index(symbol("r"),
                                                                                                                   integer(5))},
                                                                                                 {"departAfter", index(symbol("r"),
                                                                                                                       integer(6))},
                                                                                                 {"departBefore", index(symbol("r"),
                                                                                                                        integer(7))},
                                                                                                 {"arrived", index(symbol("r"),
                                                                                                                   integer(8))}})),
                                                                          "OTTlist")}})}),
                        with({._projection = projection({._items = {named(reduce("retain",
                                                                                 list({index(symbol("OTTlist"),
                                                                                             integer(0))}),
                                                                                 "i",
                                                                                 invocation({"range"},
                                                                                            {integer(1),
                                                                                             binary(BinaryOperator::Sub,
                                                                                                    invocation({"size"},
                                                                                                               {symbol("OTTlist")}),
                                                                                                    integer(1))}),
                                                                                 caseExpr(nullptr,
                                                                                          {{._tests = {{._value = binary(BinaryOperator::Equal,
                                                                                                                         lookup(index(symbol("OTTlist"),
                                                                                                                                      symbol("i")),
                                                                                                                                "nlc"),
                                                                                                                         lookup(index(symbol("retain"),
                                                                                                                                      integer(-1)),
                                                                                                                                "nlc"))}},
                                                                                            ._then = caseExpr(nullptr,
                                                                                                              {{._tests = {{._value = parenthesized(logical(LogicalOperator::And,
                                                                                                                                                            {binary(BinaryOperator::LessThanOrEqual,
                                                                                                                                                                    lookup(index(symbol("OTTlist"),
                                                                                                                                                                                 symbol("i")),
                                                                                                                                                                           "departure"),
                                                                                                                                                                    lookup(index(symbol("retain"),
                                                                                                                                                                                 integer(-1)),
                                                                                                                                                                           "departure")),
                                                                                                                                                             binary(BinaryOperator::GreaterThanOrEqual,
                                                                                                                                                                    lookup(index(symbol("OTTlist"),
                                                                                                                                                                                 symbol("i")),
                                                                                                                                                                           "arrival"),
                                                                                                                                                                    lookup(index(symbol("retain"),
                                                                                                                                                                                 integer(-1)),
                                                                                                                                                                           "arrival"))}))}},
                                                                                                                ._then = symbol("retain")},
                                                                                                               {._tests = {{._value = parenthesized(logical(LogicalOperator::And,
                                                                                                                                                            {binary(BinaryOperator::Equal,
                                                                                                                                                                    lookup(index(symbol("OTTlist"),
                                                                                                                                                                                 symbol("i")),
                                                                                                                                                                           "departure"),
                                                                                                                                                                    lookup(index(symbol("retain"),
                                                                                                                                                                                 integer(-1)),
                                                                                                                                                                           "departure")),
                                                                                                                                                             binary(BinaryOperator::LessThan,
                                                                                                                                                                    lookup(index(symbol("OTTlist"),
                                                                                                                                                                                 symbol("i")),
                                                                                                                                                                           "arrival"),
                                                                                                                                                                    lookup(index(symbol("retain"),
                                                                                                                                                                                 integer(-1)),
                                                                                                                                                                           "arrival"))}))}},
                                                                                                                ._then = binary(BinaryOperator::Add,
                                                                                                                                slice(symbol("retain"),
                                                                                                                                      nullptr,
                                                                                                                                      integer(-1)),
                                                                                                                                index(symbol("OTTlist"),
                                                                                                                                      symbol("i")))}},
                                                                                                              binary(BinaryOperator::Add,
                                                                                                                     symbol("retain"),
                                                                                                                     index(symbol("OTTlist"),
                                                                                                                           symbol("i"))))}},
                                                                                          binary(BinaryOperator::Add,
                                                                                                 symbol("retain"),
                                                                                                 index(symbol("OTTlist"),
                                                                                                       symbol("i"))))),
                                                                          "OTTlist")}})}),
                        unwind(symbol("OTTlist"), "leg"),
                        with({._projection = projection({._items = {symbol("leg")}}),
                              ._where = where(unary(UnaryOperator::Not, property({"leg", "arrived"})))}),
                        match({._pattern = pattern({path({node({._symbol = "o",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", property({"leg", "stop"})}})})}),
                                                    path({node({._symbol = "d",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("PAD")}})})})})}),
                        with({._projection = projection({._items = {symbol("leg"),
                                                                    symbol("o"),
                                                                    symbol("d"),
                                                                    named(property({"leg", "interchanges"}),
                                                                          "interchanges"),
                                                                    named(property({"leg", "departAfter"}), "depart_from"),
                                                                    named(property({"leg", "departBefore"}),
                                                                          "depart_before")}})}),
                        match({._pattern = pattern({namedPath("leg1",
                                                              {node({._symbol = "o"}),
                                                               edge({._direction = EdgePattern::Direction::Forward,
                                                                     ._labels = labels({"WED"})}),
                                                               node({._symbol = "firstStop", ._labels = labels({"MODE"})}),
                                                               walk({._direction = EdgePattern::Direction::Forward,
                                                                     ._quantifier = quantifier(0, 99),
                                                                     ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                     ._labels = labels({"NEXT"})})}}),
                                                               node({._symbol = "lastStop", ._labels = labels({"MODE"})}),
                                                               edge({._labels = labels({"ARRIVE"})}),
                                                               node({._symbol = "inter",
                                                                     ._labels = labels({"STATION"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::In,
                                                                         property({"inter", "crs"}),
                                                                         symbol("interchanges")),
                                                                  unary(UnaryOperator::Not,
                                                                        exists(query({match({._pattern = pattern({path({node({._symbol = "lastStop"}),
                                                                                                                        walk({._direction = EdgePattern::Direction::Forward,
                                                                                                                              ._quantifier = quantifier(2,
                                                                                                                                                        20),
                                                                                                                              ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                              ._labels = labelOr({labels({"NEXT"}),
                                                                                                                                                                  labels({"ARRIVE"})})})}}),
                                                                                                                        node({._symbol = "d"})})})})}))),
                                                                  caseExpr(nullptr,
                                                                           {{._tests = {{._value = entityType("firstStop",
                                                                                                              labels({"FIXED"}))}},
                                                                             ._then = parenthesized(logical(LogicalOperator::And,
                                                                                                            {binary(BinaryOperator::GreaterThan,
                                                                                                                    symbol("depart_before"),
                                                                                                                    property({"firstStop", "start"})),
                                                                                                             binary(BinaryOperator::LessThan,
                                                                                                                    symbol("depart_from"),
                                                                                                                    property({"firstStop", "end"}))}))}},
                                                                           comparisonChain(symbol("depart_before"),
                                                                                           {{BinaryOperator::GreaterThanOrEqual, property({"firstStop", "depart"})},
                                                                                            {BinaryOperator::GreaterThanOrEqual, symbol("depart_from")}})),
                                                                  binary(BinaryOperator::Equal,
                                                                         symbol("inter"),
                                                                         symbol("d"))})))})},
                       returning(projection({._items = {named(property({"leg", "service"}), "first"),
                                                        named(property({"firstStop", "service"}), "second"),
                                                        named(property({"lastStop", "arrive"}), "arrival")},
                                             ._orderBy = orderBy({{symbol("arrival"), OrderByType::ASC},
                                                                  {symbol("first"), OrderByType::ASC},
                                                                  {symbol("second"), OrderByType::ASC}})})))});

    expectParse("MATCH (o:STATION {crs: 'OXF'}), (d:STATION {crs: 'PAD'}) "
                "WITH o, d, ['DID', 'RDG', 'PAD'] AS interchanges, 470 AS depart_from, 500 AS depart_before "
                "MATCH leg1=(o)-[:WED]->(firstStop:MODE) -[:NEXT * 0..99]-> (lastStop:MODE)-[:ARRIVE]-(inter:STATION) "
                "WHERE inter.crs IN interchanges AND NOT EXISTS {(lastStop)-[:NEXT|ARRIVE]->{2,20}(d)} AND CASE "
                "WHEN firstStop:FIXED THEN (depart_before > firstStop.start AND depart_from < firstStop.end) "
                "ELSE depart_before >= firstStop.depart >= depart_from END "
                "WITH collect(leg1) AS legs "
                "WITH reduce(kept = [], l IN legs | CASE WHEN nodes(l)[-2].arrive < 505 THEN kept + l "
                "ELSE kept END) AS kept "
                "UNWIND kept AS l "
                "RETURN nodes(l)[1].service, nodes(l)[-2].arrive, length(l)",
                {query({match({._pattern = pattern({path({node({._symbol = "o",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("OXF")}})})}),
                                                    path({node({._symbol = "d",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("PAD")}})})})})}),
                        with({._projection = projection({._items = {symbol("o"),
                                                                    symbol("d"),
                                                                    named(list({string("DID"),
                                                                                string("RDG"),
                                                                                string("PAD")}),
                                                                          "interchanges"),
                                                                    named(integer(470), "depart_from"),
                                                                    named(integer(500), "depart_before")}})}),
                        match({._pattern = pattern({namedPath("leg1",
                                                              {node({._symbol = "o"}),
                                                               edge({._direction = EdgePattern::Direction::Forward,
                                                                     ._labels = labels({"WED"})}),
                                                               node({._symbol = "firstStop", ._labels = labels({"MODE"})}),
                                                               walk({._direction = EdgePattern::Direction::Forward,
                                                                     ._quantifier = quantifier(0, 99),
                                                                     ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                     ._labels = labels({"NEXT"})})}}),
                                                               node({._symbol = "lastStop", ._labels = labels({"MODE"})}),
                                                               edge({._labels = labels({"ARRIVE"})}),
                                                               node({._symbol = "inter",
                                                                     ._labels = labels({"STATION"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::In,
                                                                         property({"inter", "crs"}),
                                                                         symbol("interchanges")),
                                                                  unary(UnaryOperator::Not,
                                                                        exists(query({match({._pattern = pattern({path({node({._symbol = "lastStop"}),
                                                                                                                        walk({._direction = EdgePattern::Direction::Forward,
                                                                                                                              ._quantifier = quantifier(2,
                                                                                                                                                        20),
                                                                                                                              ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                              ._labels = labelOr({labels({"NEXT"}),
                                                                                                                                                                  labels({"ARRIVE"})})})}}),
                                                                                                                        node({._symbol = "d"})})})})}))),
                                                                  caseExpr(nullptr,
                                                                           {{._tests = {{._value = entityType("firstStop",
                                                                                                              labels({"FIXED"}))}},
                                                                             ._then = parenthesized(logical(LogicalOperator::And,
                                                                                                            {binary(BinaryOperator::GreaterThan,
                                                                                                                    symbol("depart_before"),
                                                                                                                    property({"firstStop", "start"})),
                                                                                                             binary(BinaryOperator::LessThan,
                                                                                                                    symbol("depart_from"),
                                                                                                                    property({"firstStop", "end"}))}))}},
                                                                           comparisonChain(symbol("depart_before"),
                                                                                           {{BinaryOperator::GreaterThanOrEqual, property({"firstStop", "depart"})},
                                                                                            {BinaryOperator::GreaterThanOrEqual, symbol("depart_from")}}))})))}),
                        with({._projection = projection({._items = {named(invocation({"collect"}, {symbol("leg1")}),
                                                                          "legs")}})}),
                        with({._projection = projection({._items = {named(reduce("kept",
                                                                                 list({}),
                                                                                 "l",
                                                                                 symbol("legs"),
                                                                                 caseExpr(nullptr,
                                                                                          {{._tests = {{._value = binary(BinaryOperator::LessThan,
                                                                                                                         lookup(index(invocation({"nodes"},
                                                                                                                                                 {symbol("l")}),
                                                                                                                                      integer(-2)),
                                                                                                                                "arrive"),
                                                                                                                         integer(505))}},
                                                                                            ._then = binary(BinaryOperator::Add,
                                                                                                            symbol("kept"),
                                                                                                            symbol("l"))}},
                                                                                          symbol("kept"))),
                                                                          "kept")}})}),
                        unwind(symbol("kept"), "l")},
                       returning({lookup(index(invocation({"nodes"}, {symbol("l")}), integer(1)), "service"),
                                  lookup(index(invocation({"nodes"}, {symbol("l")}), integer(-2)), "arrive"),
                                  invocation({"length"}, {symbol("l")})}))});

    expectParse("MATCH (o:STATION {crs: 'OXF'}), (d:STATION {crs: 'PAD'}) "
                "WITH o, d, ['DID', 'RDG', 'PAD'] AS interchanges, 470 AS depart_from, 500 AS depart_before "
                "MATCH leg1=(o)-[:WED]->(firstStop:MODE) -[:NEXT * 0..99]-> (lastStop:MODE)-[:ARRIVE]-(inter:STATION) "
                "WHERE inter.crs IN interchanges AND NOT EXISTS {(lastStop)-[:NEXT|ARRIVE]->{2,20}(d)} AND CASE "
                "WHEN firstStop:FIXED THEN (depart_before > firstStop.start AND depart_from < firstStop.end) "
                "ELSE depart_before >= firstStop.depart >= depart_from END "
                "WITH collect(leg1) AS legs "
                "RETURN size(legs), reduce(longest = 0, l IN legs | CASE WHEN length(l) > longest THEN length(l) "
                "ELSE longest END), reduce(hops = 0, l IN legs | hops + length(l)), "
                "reduce(callingPoints = 0, l IN legs | callingPoints + size([n IN nodes(l) "
                "WHERE n:MODE]))",
                {query({match({._pattern = pattern({path({node({._symbol = "o",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("OXF")}})})}),
                                                    path({node({._symbol = "d",
                                                                ._labels = labels({"STATION"}),
                                                                ._properties = properties({{"crs", string("PAD")}})})})})}),
                        with({._projection = projection({._items = {symbol("o"),
                                                                    symbol("d"),
                                                                    named(list({string("DID"),
                                                                                string("RDG"),
                                                                                string("PAD")}),
                                                                          "interchanges"),
                                                                    named(integer(470), "depart_from"),
                                                                    named(integer(500), "depart_before")}})}),
                        match({._pattern = pattern({namedPath("leg1",
                                                              {node({._symbol = "o"}),
                                                               edge({._direction = EdgePattern::Direction::Forward,
                                                                     ._labels = labels({"WED"})}),
                                                               node({._symbol = "firstStop", ._labels = labels({"MODE"})}),
                                                               walk({._direction = EdgePattern::Direction::Forward,
                                                                     ._quantifier = quantifier(0, 99),
                                                                     ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                     ._labels = labels({"NEXT"})})}}),
                                                               node({._symbol = "lastStop", ._labels = labels({"MODE"})}),
                                                               edge({._labels = labels({"ARRIVE"})}),
                                                               node({._symbol = "inter",
                                                                     ._labels = labels({"STATION"})})})},
                                                   where(logical(LogicalOperator::And,
                                                                 {binary(BinaryOperator::In,
                                                                         property({"inter", "crs"}),
                                                                         symbol("interchanges")),
                                                                  unary(UnaryOperator::Not,
                                                                        exists(query({match({._pattern = pattern({path({node({._symbol = "lastStop"}),
                                                                                                                        walk({._direction = EdgePattern::Direction::Forward,
                                                                                                                              ._quantifier = quantifier(2,
                                                                                                                                                        20),
                                                                                                                              ._hops = {edge({._direction = EdgePattern::Direction::Forward,
                                                                                                                                              ._labels = labelOr({labels({"NEXT"}),
                                                                                                                                                                  labels({"ARRIVE"})})})}}),
                                                                                                                        node({._symbol = "d"})})})})}))),
                                                                  caseExpr(nullptr,
                                                                           {{._tests = {{._value = entityType("firstStop",
                                                                                                              labels({"FIXED"}))}},
                                                                             ._then = parenthesized(logical(LogicalOperator::And,
                                                                                                            {binary(BinaryOperator::GreaterThan,
                                                                                                                    symbol("depart_before"),
                                                                                                                    property({"firstStop", "start"})),
                                                                                                             binary(BinaryOperator::LessThan,
                                                                                                                    symbol("depart_from"),
                                                                                                                    property({"firstStop", "end"}))}))}},
                                                                           comparisonChain(symbol("depart_before"),
                                                                                           {{BinaryOperator::GreaterThanOrEqual, property({"firstStop", "depart"})},
                                                                                            {BinaryOperator::GreaterThanOrEqual, symbol("depart_from")}}))})))}),
                        with({._projection = projection({._items = {named(invocation({"collect"}, {symbol("leg1")}),
                                                                          "legs")}})})},
                       returning({invocation({"size"}, {symbol("legs")}),
                                  reduce("longest",
                                         integer(0),
                                         "l",
                                         symbol("legs"),
                                         caseExpr(nullptr,
                                                  {{._tests = {{._value = binary(BinaryOperator::GreaterThan,
                                                                                 invocation({"length"}, {symbol("l")}),
                                                                                 symbol("longest"))}},
                                                    ._then = invocation({"length"}, {symbol("l")})}},
                                                  symbol("longest"))),
                                  reduce("hops",
                                         integer(0),
                                         "l",
                                         symbol("legs"),
                                         binary(BinaryOperator::Add,
                                                symbol("hops"),
                                                invocation({"length"}, {symbol("l")}))),
                                  reduce("callingPoints",
                                         integer(0),
                                         "l",
                                         symbol("legs"),
                                         binary(BinaryOperator::Add,
                                                symbol("callingPoints"),
                                                invocation({"size"},
                                                           {listComprehension("n",
                                                                              invocation({"nodes"}, {symbol("l")}),
                                                                              entityType("n", labels({"MODE"})))})))}))});
}

TEST_F(ParseEdgeCaseTest, parsesCallsAndSubqueries) {
    expectError("OPTIONAL 5",
                "     1 | OPTIONAL 5\n"
                "       |          ^\n"
                "-------* syntax error, unexpected DIGIT, expecting MATCH or CALL");

    expectParse("OPTIONAL MATCH (n) RETURN n",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})}), ._optional = true})},
                       returning({symbol("n")}))});

    expectError("OPTIONAL CALL 5",
                "     1 | OPTIONAL CALL 5\n"
                "       |               ^\n"
                "-------* syntax error, unexpected DIGIT, expecting OPAREN or OBRACE or a name");

    expectError("OPTIONAL CALL f() 5",
                "     1 | OPTIONAL CALL f() 5\n"
                "       |                   ^\n"
                "-------* syntax error, unexpected DIGIT");

    expectError("CALL 5",
                "     1 | CALL 5\n"
                "       |      ^\n"
                "-------* syntax error, unexpected DIGIT, expecting OPAREN or OBRACE or a name");

    expectParse("CALL f()", {query({call({._invocation = invocation({"f"}, {}), ._standalone = true})})});

    expectParse("CALL f() YIELD *",
                {query({call({._invocation = invocation({"f"}, {}), ._yield = yieldAll(), ._standalone = true})})});

    expectParse("CALL f() YIELD a AS b, c WHERE c > 1 RETURN b",
                {query({call({._invocation = invocation({"f"}, {}),
                              ._yield = yield({yieldItem("a", "b"), symbol("c")},
                                              where(binary(BinaryOperator::GreaterThan, symbol("c"), integer(1))))})},
                       returning({symbol("b")}))});

    expectError("CALL db.labels",
                "     1 | CALL db.labels\n"
                "       |         ^^^^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting OPAREN or DOT");

    expectParse("CALL { RETURN 1 }", {query({callSubquery({._body = query({}, returning({integer(1)}))})})});

    expectParse("CALL () { RETURN 1 }",
                {query({callSubquery({._body = query({}, returning({integer(1)})), ._scope = true})})});

    expectParse("CALL (*) { RETURN 1 }",
                {query({callSubquery({._body = query({}, returning({integer(1)})),
                                      ._scope = true,
                                      ._importsEverything = true})})});

    expectParse("CALL (a, b) { RETURN 1 }",
                {query({callSubquery({._body = query({}, returning({integer(1)})),
                                      ._imports = {"a", "b"},
                                      ._scope = true})})});

    expectError("CALL (5",
                "     1 | CALL (5\n"
                "       |       ^\n"
                "-------* syntax error, unexpected DIGIT, expecting CPAREN or MULT or a name");

    expectParse("MATCH (n) CALL (n) { RETURN 1 } RETURN n",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})}),
                        callSubquery({._body = query({}, returning({integer(1)})), ._imports = {"n"}, ._scope = true})},
                       returning({symbol("n")}))});

    expectParse("OPTIONAL CALL (n) { RETURN 1 }",
                {query({callSubquery({._body = query({}, returning({integer(1)})),
                                      ._imports = {"n"},
                                      ._scope = true,
                                      ._optional = true})})});

    expectError("CALL 99999999999999999999",
                "     1 | CALL 99999999999999999999\n"
                "       |      ^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Decimal literal exceeded int64 range.");

    expectParse("RETURN EXISTS { }", {query({}, returning({mapProjection("EXISTS", {})}))});

    expectError("RETURN EXISTS { 1 }",
                "     1 | RETURN EXISTS { 1 }\n"
                "       |                 ^\n"
                "-------* syntax error, unexpected DIGIT, expecting OPAREN or OBRACE or a name");

    expectError("RETURN COUNT { $x }",
                "     1 | RETURN COUNT { $x }\n"
                "       |                ^\n"
                "-------* syntax error, unexpected DOLLAR, expecting OPAREN or OBRACE or a name");

    expectParse("RETURN EXISTS { MATCH }",
                {query({},
                       returning({mapProjection("EXISTS",
                                                {{._kind = MapProjectionExpr::ItemKind::Variable,
                                                  ._key = name("MATCH"),
                                                  ._value = symbol("MATCH")}})}))});

    expectError("RETURN EXISTS { MATCH 5 }",
                "     1 | RETURN EXISTS { MATCH 5 }\n"
                "       |                       ^\n"
                "-------* syntax error, unexpected DIGIT, expecting OPAREN or ASSIGN or a name");

    expectError("RETURN EXISTS { OPTIONAL 5 }",
                "     1 | RETURN EXISTS { OPTIONAL 5 }\n"
                "       |                          ^\n"
                "-------* syntax error, unexpected DIGIT, expecting ASSIGN or MATCH or CALL");

    expectError("RETURN EXISTS { CALL 5 }",
                "     1 | RETURN EXISTS { CALL 5 }\n"
                "       |                      ^\n"
                "-------* syntax error, unexpected DIGIT, expecting OPAREN or OBRACE or ASSIGN or a name");

    expectParse("RETURN EXISTS { foo }",
                {query({},
                       returning({mapProjection("EXISTS",
                                                {{._kind = MapProjectionExpr::ItemKind::Variable,
                                                  ._key = name("foo"),
                                                  ._value = symbol("foo")}})}))});

    expectError("RETURN EXISTS { foo (a) }",
                "     1 | RETURN EXISTS { foo (a) }\n"
                "       |                     ^\n"
                "-------* syntax error, unexpected OPAREN, expecting ASSIGN");

    expectParse("RETURN EXISTS { foo = (a) }",
                {query({},
                       returning({exists(query({match({._pattern = pattern({namedPath("foo",
                                                                                      {node({._symbol = "a"})})})})}))}))});

    expectParse("RETURN EXISTS { match = (a) }",
                {query({},
                       returning({exists(query({match({._pattern = pattern({namedPath("match",
                                                                                      {node({._symbol = "a"})})})})}))}))});

    expectParse("RETURN EXISTS { (a)-->(b) WHERE a.x > 1 }",
                {query({},
                       returning({exists(query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                                                  edge({._direction = EdgePattern::Direction::Forward}),
                                                                                  node({._symbol = "b"})})},
                                                                           where(binary(BinaryOperator::GreaterThan,
                                                                                        property({"a", "x"}),
                                                                                        integer(1))))})}))}))});

    expectParse("RETURN EXISTS { MATCH (a) RETURN a }",
                {query({},
                       returning({exists(query({match({._pattern = pattern({path({node({._symbol = "a"})})})})},
                                               returning({symbol("a")})))}))});

    expectParse("RETURN EXISTS { WHEN true THEN RETURN 1 }",
                {query({},
                       returning({exists(conditional({{._condition = boolean(true),
                                                       ._query = query({}, returning({integer(1)}))}}))}))});

    expectError("RETURN EXISTS { 99999999999999999999 }",
                "     1 | RETURN EXISTS { 99999999999999999999 }\n"
                "       |                 ^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Decimal literal exceeded int64 range.");

    expectError("RETURN EXISTS { MATCH 99999999999999999999 }",
                "     1 | RETURN EXISTS { MATCH 99999999999999999999 }\n"
                "       |                       ^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Decimal literal exceeded int64 range.");

    expectError("RETURN EXISTS(n.x)",
                "     1 | RETURN EXISTS(n.x)\n"
                "       |               ^^^\n"
                "-------* exists() takes a pattern: test a property with IS NOT NULL");

    expectParse("RETURN EXISTS((a)-->(b))",
                {query({},
                       returning({patternPredicate(pattern({path({node({._symbol = "a"}),
                                                                  edge({._direction = EdgePattern::Direction::Forward}),
                                                                  node({._symbol = "b"})})}))}))});

    expectParse("RETURN COUNT { (a) }",
                {query({},
                       returning({countSubquery(query({match({._pattern = pattern({path({node({._symbol = "a"})})})})}))}))});

    expectParse("RETURN COLLECT { RETURN 1 }",
                {query({}, returning({collectSubquery(query({}, returning({integer(1)})))}))});

    expectError("MATCH (n) OPTIONAL 5",
                "     1 | MATCH (n) OPTIONAL 5\n"
                "       |                    ^\n"
                "-------* syntax error, unexpected DIGIT, expecting MATCH or CALL");
}

TEST_F(ParseEdgeCaseTest, parsesValueTestsAndCases) {
    expectError("RETURN 1 IS NOT 5",
                "     1 | RETURN 1 IS NOT 5\n"
                "       |             ^^^^^\n"
                "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED");

    expectError("RETURN 'a' IS NOT NFC 5",
                "     1 | RETURN 'a' IS NOT NFC 5\n"
                "       |               ^^^^^^^^^\n"
                "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED");

    expectError("RETURN 1 IS NOT (NULL)",
                "     1 | RETURN 1 IS NOT (NULL)\n"
                "       |             ^^^^^\n"
                "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED");

    expectError("RETURN 1 IS NFC 5",
                "     1 | RETURN 1 IS NFC 5\n"
                "       |             ^^^^^\n"
                "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED");

    expectParse("RETURN 1 IS NULL", {query({}, returning({binary(BinaryOperator::IsNull, integer(1), null())}))});

    expectParse("RETURN 1 IS NOT NULL", {query({}, returning({binary(BinaryOperator::IsNotNull, integer(1), null())}))});

    expectParse("RETURN 'a' IS NORMALIZED",
                {query({}, returning({invocation({"is_normalized"}, {string("a"), normalForm(UnicodeNormalForm::NFC)})}))});

    expectParse("RETURN 'a' IS NOT NFKD NORMALIZED",
                {query({},
                       returning({unary(UnaryOperator::Not,
                                        invocation({"is_normalized"},
                                                   {string("a"), normalForm(UnicodeNormalForm::NFKD)}))}))});

    expectError("RETURN 1 IS NOT 99999999999999999999",
                "     1 | RETURN 1 IS NOT 99999999999999999999\n"
                "       |                 ^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Decimal literal exceeded int64 range.");

    expectError("RETURN 1 IS NFC 99999999999999999999",
                "     1 | RETURN 1 IS NFC 99999999999999999999\n"
                "       |                 ^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Decimal literal exceeded int64 range.");

    expectError("RETURN 1 IS NFC",
                "     1 | RETURN 1 IS NFC\n"
                "       |             ^^^\n"
                "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED");

    expectError("RETURN 1 IS NOT",
                "     1 | RETURN 1 IS NOT\n"
                "       |             ^^^\n"
                "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED");

    expectParse("RETURN 1 IS NOT NULL IS NULL",
                {query({},
                       returning({binary(BinaryOperator::IsNull,
                                         binary(BinaryOperator::IsNotNull, integer(1), null()),
                                         null())}))});

    expectParse("RETURN 1 < 2 <= 3 IS NULL = true",
                {query({},
                       returning({comparisonChain(integer(1),
                                                  {{BinaryOperator::LessThan, integer(2)},
                                                   {BinaryOperator::LessThanOrEqual, binary(BinaryOperator::IsNull, integer(3), null())},
                                                   {BinaryOperator::Equal, boolean(true)}})}))});

    expectError("RETURN CASE WHEN IS NOT 5 THEN 1 END",
                "     1 | RETURN CASE WHEN IS NOT 5 THEN 1 END\n"
                "       |                         ^\n"
                "-------* syntax error, unexpected DIGIT, expecting NULL_");

    expectParse("RETURN CASE 1 WHEN IS NULL THEN 2 END",
                {query({},
                       returning({caseExpr(integer(1),
                                           {{._tests = {{._kind = CaseExpr::TestKind::IsNull}}, ._then = integer(2)}})}))});

    expectParse("RETURN CASE 1 WHEN < 2, = 3 THEN 4 ELSE 5 END",
                {query({},
                       returning({caseExpr(integer(1),
                                           {{._tests = {{._value = integer(2),
                                                         ._operator = BinaryOperator::LessThan,
                                                         ._kind = CaseExpr::TestKind::Comparison},
                                                        {._value = integer(3),
                                                         ._operator = BinaryOperator::Equal,
                                                         ._kind = CaseExpr::TestKind::Comparison}},
                                             ._then = integer(4)}},
                                           integer(5))}))});

    expectError("RETURN CASE WHEN x IS NOT 5 THEN 1 END",
                "     1 | RETURN CASE WHEN x IS NOT 5 THEN 1 END\n"
                "       |                       ^^^^^\n"
                "-------* The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED");
}

TEST_F(ParseEdgeCaseTest, parsesUpdatingClauses) {
    expectParse("MATCH (vector) DELETE vector",
                {query({match({._pattern = pattern({path({node({._symbol = "vector"})})})}),
                        deleteStmt({symbol("vector")})})});

    expectError("MATCH (n) REMOVE n:A",
                "     1 | MATCH (n) REMOVE n:A\n"
                "       |                  ^^^\n"
                "-------* Not implemented: REMOVE of a label");

    expectError("MATCH (n) REMOVE n:A 5",
                "     1 | MATCH (n) REMOVE n:A 5\n"
                "       |                      ^\n"
                "-------* syntax error, unexpected DIGIT");

    expectError("MATCH (n) REMOVE n:A, n.x",
                "     1 | MATCH (n) REMOVE n:A, n.x\n"
                "       |                  ^^^\n"
                "-------* Not implemented: REMOVE of a label");

    expectError("MATCH (n) REMOVE n IS A",
                "     1 | MATCH (n) REMOVE n IS A\n"
                "       |                  ^^^^^^\n"
                "-------* Not implemented: REMOVE of a label");

    expectParse("MATCH (n) REMOVE (n).x",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})}), remove({property({"n", "x"})})})});

    expectError("MATCH (n) REMOVE (1).x",
                "     1 | MATCH (n) REMOVE (1).x\n"
                "       |                  ^^^\n"
                "-------* Invalid property access. Only a variable can be parenthesized on the left of '.'");

    expectError("MATCH (n) REMOVE n 5",
                "     1 | MATCH (n) REMOVE n 5\n"
                "       |                    ^\n"
                "-------* syntax error, unexpected DIGIT, expecting COLON or DOT or IS");

    expectParse("MATCH (n) SET n = 1",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})}),
                        set({replaceProperties("n", integer(1))})})});

    expectError("MATCH (n) SET n 5",
                "     1 | MATCH (n) SET n 5\n"
                "       |                 ^\n"
                "-------* syntax error, unexpected DIGIT");

    expectParse("MATCH (n) SET n += {a: 1}",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})}),
                        set({addProperties("n", map({{"a", integer(1)}}))})})});

    expectParse("MATCH (n) SET n:A:B",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})}),
                        set({setLabels(entityType("n", labels({"A", "B"})))})})});

    expectParse("MATCH (n) SET (n).x = 1, n.y.z = 2",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})}),
                        set({setProperty(property({"n", "x"}), integer(1)),
                             setProperty(property({"n", "y", "z"}), integer(2))})})});

    expectError("MATCH (n) SET (1).x = 1",
                "     1 | MATCH (n) SET (1).x = 1\n"
                "       |               ^^^\n"
                "-------* Invalid property access. Only a variable can be parenthesized on the left of '.'");

    expectParse("MERGE (a) ON CREATE SET a.x = 1 ON MATCH SET a.y = 2 ON CREATE SET a.z = 3",
                {query({merge(pattern({path({node({._symbol = "a"})})}),
                              set({setProperty(property({"a", "x"}), integer(1)),
                                   setProperty(property({"a", "z"}), integer(3))}),
                              set({setProperty(property({"a", "y"}), integer(2))}))})});

    expectError("MERGE (a) ON 5",
                "     1 | MERGE (a) ON 5\n"
                "       |              ^\n"
                "-------* syntax error, unexpected DIGIT, expecting CREATE or MATCH");

    expectParse("MERGE p = (a)", {query({merge(pattern({namedPath("p", {node({._symbol = "a"})})}))})});

    expectParse("FOREACH (x IN [1] | CREATE (a) SET a.x = 1 MERGE (b) DELETE b REMOVE a.x FOREACH (y IN [2] | CREATE (c)))",
                {query({callSubquery({._body = query({unwind(list({integer(1)}), "x"),
                                                      createStmt(pattern({path({node({._symbol = "a"})})})),
                                                      set({setProperty(property({"a", "x"}), integer(1))}),
                                                      merge(pattern({path({node({._symbol = "b"})})})),
                                                      deleteStmt({symbol("b")}),
                                                      remove({property({"a", "x"})}),
                                                      callSubquery({._body = query({unwind(list({integer(2)}), "y"),
                                                                                    createStmt(pattern({path({node({._symbol = "c"})})}))}),
                                                                    ._scope = true,
                                                                    ._importsEverything = true})}),
                                      ._scope = true,
                                      ._importsEverything = true})})});

    expectError("FOREACH (x IN [1] | RETURN 1)",
                "     1 | FOREACH (x IN [1] | RETURN 1)\n"
                "       |                     ^^^^^^\n"
                "-------* syntax error, unexpected RETURN");

    expectParse("MATCH (n) DETACH DELETE n, m",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})}),
                        detachDelete({symbol("n"), symbol("m")})})});

    expectError("MATCH (n) DETACH 5",
                "     1 | MATCH (n) DETACH 5\n"
                "       |                  ^\n"
                "-------* syntax error, unexpected DIGIT, expecting DELETE");

    expectParse("CREATE (a)-[:T {x: 1}]->(b), (c)",
                {query({createStmt(pattern({path({node({._symbol = "a"}),
                                                  edge({._direction = EdgePattern::Direction::Forward,
                                                        ._labels = labels({"T"}),
                                                        ._properties = properties({{"x", integer(1)}})}),
                                                  node({._symbol = "b"})}),
                                            path({node({._symbol = "c"})})}))})});
}

TEST_F(ParseEdgeCaseTest, parsesRelationships) {
    expectParse("MATCH (a)-->(b) RETURN 1 - [1, 2][0]",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "b"})})})})},
                       returning({binary(BinaryOperator::Sub,
                                         integer(1),
                                         index(list({integer(1), integer(2)}), integer(0)))}))});

    expectParse("RETURN 1 - [1, 2]",
                {query({}, returning({binary(BinaryOperator::Sub, integer(1), list({integer(1), integer(2)}))}))});

    expectParse("RETURN -[1, 2][0]",
                {query({}, returning({unary(UnaryOperator::Minus, index(list({integer(1), integer(2)}), integer(0)))}))});

    expectParse("WITH 1 AS x WHERE x <--1 RETURN x",
                {query({with({._projection = projection({._items = {named(integer(1), "x")}}),
                              ._where = where(binary(BinaryOperator::LessThan, symbol("x"), integer(1)))})},
                       returning({symbol("x")}))});

    expectParse("WITH 1 AS x WHERE (x) < - -2 RETURN x",
                {query({with({._projection = projection({._items = {named(integer(1), "x")}}),
                              ._where = where(binary(BinaryOperator::LessThan, parenthesized(symbol("x")), integer(2)))})},
                       returning({symbol("x")}))});

    expectParse("RETURN (1) - [1, 2][1]",
                {query({},
                       returning({binary(BinaryOperator::Sub,
                                         parenthesized(integer(1)),
                                         index(list({integer(1), integer(2)}), integer(1)))}))});

    expectParse("MATCH (a {x: -[1][0]}) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a",
                                                                ._properties = properties({{"x", unary(UnaryOperator::Minus,
                                                                                                       index(list({integer(1)}),
                                                                                                             integer(0)))}})})})})})},
                       returning({symbol("a")}))});

    {
        BinaryExpr* filter = binary(BinaryOperator::GreaterThan,
                                     property({"e", "d"}),
                                     unary(UnaryOperator::Minus, index(list({integer(1)}), integer(0))));

        expectParse("MATCH ()-[e WHERE e.d > -[1][0]]->() RETURN e",
                    {query({match({._pattern = pattern({path({node({}),
                                                              edge({._direction = EdgePattern::Direction::Forward,
                                                                    ._symbol = "e",
                                                                    ._where = where(filter)}),
                                                              node({})})},
                                                       where(filter))})},
                           returning({symbol("e")}))});
    }

    expectParse("MATCH (a)-\n->(b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          edge({._direction = EdgePattern::Direction::Forward}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectParse("MATCH (a) < - [e] - (b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._symbol = "e"}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectError("MATCH (a)-(b) RETURN a",
                "     1 | MATCH (a)-(b) RETURN a\n"
                "       |           ^\n"
                "-------* syntax error, unexpected OPAREN, expecting OBRACK or SUB");

    expectError("MATCH (a)<(b) RETURN a",
                "     1 | MATCH (a)<(b) RETURN a\n"
                "       |           ^\n"
                "-------* syntax error, unexpected OPAREN, expecting SUB");

    expectError("MATCH (a)<-- RETURN a",
                "     1 | MATCH (a)<-- RETURN a\n"
                "       |              ^^^^^^\n"
                "-------* syntax error, unexpected RETURN");

    expectError("MATCH (a)--> RETURN a",
                "     1 | MATCH (a)--> RETURN a\n"
                "       |              ^^^^^^\n"
                "-------* syntax error, unexpected RETURN, expecting OPAREN or OBRACE or PLUS or MULT");

    expectError("MATCH (a)-",
                "     1 | MATCH (a)-\n"
                "       |          ^\n"
                "-------* syntax error, unexpected PROG_END, expecting OBRACK or SUB");

    expectError("MATCH (a)-[e]-",
                "     1 | MATCH (a)-[e]-\n"
                "       |              ^\n"
                "-------* syntax error, unexpected PROG_END");

    expectParse("MATCH (a)-[e]->(b)<-[f]-(c)--(d)<-->(e)-[]-(f) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          edge({._direction = EdgePattern::Direction::Forward,
                                                                ._symbol = "e"}),
                                                          node({._symbol = "b"}),
                                                          edge({._direction = EdgePattern::Direction::Backward,
                                                                ._symbol = "f"}),
                                                          node({._symbol = "c"}),
                                                          edge({}),
                                                          node({._symbol = "d"}),
                                                          edge({}),
                                                          node({._symbol = "e"}),
                                                          edge({}),
                                                          node({._symbol = "f"})})})})},
                       returning({symbol("a")}))});

    expectParse("MATCH (a)-[*]->(b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(1, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})}}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectParse("MATCH (a)-[*2]->(b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(2, 2),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})}}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectParse("MATCH (a)-[*2..]->(b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(2, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})}}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectParse("MATCH (a)-[*..3]->(b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(1, 3),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})}}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectError("MATCH (a)-[*..]->(b) RETURN a",
                "     1 | MATCH (a)-[*..]->(b) RETURN a\n"
                "       |               ^\n"
                "-------* syntax error, unexpected CBRACK, expecting DIGIT");

    expectError("MATCH (a)-[e*1..2]->{1,3}(b) RETURN a",
                "     1 | MATCH (a)-[e*1..2]->{1,3}(b) RETURN a\n"
                "       |                     ^^^^^\n"
                "-------* A relationship takes one length quantifier: either [e*1..3] inside the brackets or {1,3} after them");

    expectParse("MATCH (a)-->{1,3}(b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(1, 3),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})}}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectParse("MATCH (a)-->+(b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(1, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})}}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectParse("MATCH (a)-->*(b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(0, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})}}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectParse("MATCH (a)-->{,}(b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(0, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})}}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectParse("MATCH (a)-->{2}(b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(2, 2),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})}}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectParse("MATCH (a)-->{2,}(b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(2, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})}}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectParse("MATCH (a)-->{,2}(b) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(0, 2),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})}}),
                                                          node({._symbol = "b"})})})})},
                       returning({symbol("a")}))});

    expectError("MATCH (a)-->{x}(b) RETURN a",
                "     1 | MATCH (a)-->{x}(b) RETURN a\n"
                "       |              ^\n"
                "-------* syntax error, unexpected ID, expecting COMMA or DIGIT");

    expectParse("MATCH (a)((b)-->(c))+(d) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(1, QuantifiedPath::UNBOUNDED),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})},
                                                                ._hopNodes = {node({._symbol = "b"}),
                                                                              node({._symbol = "c"})}}),
                                                          node({._symbol = "d"})})})})},
                       returning({symbol("a")}))});

    expectError("MATCH (a)((b))+(d) RETURN a",
                "     1 | MATCH (a)((b))+(d) RETURN a\n"
                "       |           ^^^\n"
                "-------* A quantified pattern repeats at least one relationship");

    expectError("MATCH (a)((b)-[*2]->(c))+(d) RETURN a",
                "     1 | MATCH (a)((b)-[*2]->(c))+(d) RETURN a\n"
                "       |           ^^^^^^^^^^^^^\n"
                "-------* A relationship takes one length quantifier: either [e*1..3] inside the brackets or {1,3} after them");

    expectError("MATCH (a)((b)-->{2}(c)-->(e))+(d) RETURN a",
                "     1 | MATCH (a)((b)-->{2}(c)-->(e))+(d) RETURN a\n"
                "       |           ^^^^^^^^^^^^^^^^^^\n"
                "-------* A quantified pattern cannot repeat a pattern that is quantified itself");

    expectParse("MATCH (a)((b)-->(c) WHERE b.x > 1){1,2}(d) RETURN a",
                {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                          walk({._direction = EdgePattern::Direction::Forward,
                                                                ._quantifier = quantifier(1, 2),
                                                                ._hops = {edge({._direction = EdgePattern::Direction::Forward})},
                                                                ._hopNodes = {node({._symbol = "b"}),
                                                                              node({._symbol = "c"})},
                                                                ._hopWhere = where(binary(BinaryOperator::GreaterThan,
                                                                                          property({"b", "x"}),
                                                                                          integer(1)))}),
                                                          node({._symbol = "d"})})})})},
                       returning({symbol("a")}))});

    expectError("MATCH (a) WHERE (a)-[*2]->{1,3}(b) RETURN a",
                "     1 | MATCH (a) WHERE (a)-[*2]->{1,3}(b) RETURN a\n"
                "       |                           ^^^^^\n"
                "-------* A relationship takes one length quantifier: either [e*1..3] inside the brackets or {1,3} after them");

    expectParse("MATCH p = (a)-->(b) RETURN p",
                {query({match({._pattern = pattern({namedPath("p",
                                                              {node({._symbol = "a"}),
                                                               edge({._direction = EdgePattern::Direction::Forward}),
                                                               node({._symbol = "b"})})})})},
                       returning({symbol("p")}))});

    expectError("MATCH p = 5",
                "     1 | MATCH p = 5\n"
                "       |           ^\n"
                "-------* syntax error, unexpected DIGIT, expecting OPAREN");

    expectError("MATCH p",
                "     1 | MATCH p\n"
                "       |       ^\n"
                "-------* syntax error, unexpected PROG_END, expecting ASSIGN");
}

TEST_F(ParseEdgeCaseTest, parsesParenthesizedAtoms) {
    expectError("RETURN (1)-->(b)",
                "     1 | RETURN (1)-->(b)\n"
                "       |        ^\n"
                "-------* Invalid path expr. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'");

    expectError("RETURN (1)-->(b) 5",
                "     1 | RETURN (1)-->(b) 5\n"
                "       |                  ^\n"
                "-------* syntax error, unexpected DIGIT");

    expectError("RETURN (1)-->(b)-->(c)",
                "     1 | RETURN (1)-->(b)-->(c)\n"
                "       |        ^\n"
                "-------* Invalid path expr. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'");

    expectError("RETURN (n.x)-->(b)",
                "     1 | RETURN (n.x)-->(b)\n"
                "       |        ^\n"
                "-------* Invalid path expr. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'");

    expectParse("RETURN (n:A)-->(b)",
                {query({},
                       returning({patternPredicate(pattern({path({node({._symbol = "n", ._labels = labels({"A"})}),
                                                                  edge({._direction = EdgePattern::Direction::Forward}),
                                                                  node({._symbol = "b"})})}))}))});

    expectParse("RETURN (n {a: 1})-->(b)",
                {query({},
                       returning({patternPredicate(pattern({path({node({._symbol = "n",
                                                                        ._properties = properties({{"a", integer(1)}})}),
                                                                  edge({._direction = EdgePattern::Direction::Forward}),
                                                                  node({._symbol = "b"})})}))}))});

    expectParse("RETURN ({a: 1})-->(b)",
                {query({},
                       returning({patternPredicate(pattern({path({node({._properties = properties({{"a", integer(1)}})}),
                                                                  edge({._direction = EdgePattern::Direction::Forward}),
                                                                  node({._symbol = "b"})})}))}))});

    expectError("RETURN (n {.x})-->(b)",
                "     1 | RETURN (n {.x})-->(b)\n"
                "       |        ^\n"
                "-------* Invalid path expr. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'");

    expectError("RETURN (n {.x})-->(b) 5",
                "     1 | RETURN (n {.x})-->(b) 5\n"
                "       |                       ^\n"
                "-------* syntax error, unexpected DIGIT");

    expectError("RETURN (n {.x} WHERE true)",
                "     1 | RETURN (n {.x} WHERE true)\n"
                "       |           ^^^^\n"
                "-------* Invalid node pattern. Its properties are 'key: value' entries");

    expectError("RETURN (n {.x} WHERE true) 5",
                "     1 | RETURN (n {.x} WHERE true) 5\n"
                "       |           ^^^^\n"
                "-------* Invalid node pattern. Its properties are 'key: value' entries");

    expectError("RETURN (n {.x} WHERE true).y",
                "     1 | RETURN (n {.x} WHERE true).y\n"
                "       |           ^^^^\n"
                "-------* Invalid node pattern. Its properties are 'key: value' entries");

    {
        LiteralExpr* filter = boolean(true);

        expectParse("RETURN (n {a: 1} WHERE true)",
                    {query({},
                           returning({patternPredicate(pattern({path({node({._symbol = "n",
                                                                            ._properties = properties({{"a", integer(1)}}),
                                                                            ._where = where(filter)})})},
                                                               where(filter)))}))});
    }

    {
        LiteralExpr* filter = boolean(true);

        expectParse("RETURN ({a: 1} WHERE true)",
                    {query({},
                           returning({patternPredicate(pattern({path({node({._properties = properties({{"a", integer(1)}}),
                                                                            ._where = where(filter)})})},
                                                               where(filter)))}))});
    }

    expectParse("RETURN ({a: 1})", {query({}, returning({parenthesized(map({{"a", integer(1)}}))}))});

    {
        LiteralExpr* filter = boolean(true);

        expectParse("RETURN (n:A|B WHERE true)",
                    {query({},
                           returning({patternPredicate(pattern({path({node({._symbol = "n",
                                                                            ._labels = labelOr({labels({"A"}), labels({"B"})}),
                                                                            ._where = where(filter)})})},
                                                               where(filter)))}))});
    }

    {
        LiteralExpr* filter = boolean(true);

        expectParse("RETURN (n:(A|B) WHERE true)",
                    {query({},
                           returning({patternPredicate(pattern({path({node({._symbol = "n",
                                                                            ._labels = labelOr({labels({"A"}), labels({"B"})}),
                                                                            ._where = where(filter)})})},
                                                               where(filter)))}))});
    }

    {
        LiteralExpr* filter = boolean(true);

        expectParse("RETURN (n:!A&B WHERE true)",
                    {query({},
                           returning({patternPredicate(pattern({path({node({._symbol = "n",
                                                                            ._labels = labelAnd({labelNot(labels({"A"})),
                                                                                                 labels({"B"})}),
                                                                            ._where = where(filter)})})},
                                                               where(filter)))}))});
    }

    {
        LiteralExpr* filter = boolean(true);

        expectParse("RETURN (n:$x WHERE true)",
                    {query({},
                           returning({patternPredicate(pattern({path({node({._symbol = "n",
                                                                            ._labels = labelParameter("x"),
                                                                            ._where = where(filter)})})},
                                                               where(filter)))}))});
    }

    expectParse("RETURN (n:$1 {a: 1})",
                {query({},
                       returning({patternPredicate(pattern({path({node({._symbol = "n",
                                                                        ._labels = labelParameter("1"),
                                                                        ._properties = properties({{"a", integer(1)}})})})}))}))});

    expectParse("RETURN (n:A)", {query({}, returning({parenthesized(entityType("n", labels({"A"})))}))});

    expectParse("RETURN (n:A {a: 1})-->(m)",
                {query({},
                       returning({patternPredicate(pattern({path({node({._symbol = "n",
                                                                        ._labels = labels({"A"}),
                                                                        ._properties = properties({{"a", integer(1)}})}),
                                                                  edge({._direction = EdgePattern::Direction::Forward}),
                                                                  node({._symbol = "m"})})}))}))});

    expectParse("RETURN (count {MATCH (n)})",
                {query({},
                       returning({parenthesized(countSubquery(query({match({._pattern = pattern({path({node({._symbol = "n"})})})})})))}))});

    expectParse("RETURN (not where)", {query({}, returning({parenthesized(unary(UnaryOperator::Not, symbol("where")))}))});

    {
        SymbolExpr* filter = symbol("x");

        expectParse("RETURN (count WHERE x)",
                    {query({},
                           returning({patternPredicate(pattern({path({node({._symbol = "count", ._where = where(filter)})})},
                                                               where(filter)))}))});
    }

    expectError("RETURN (null WHERE x)",
                "     1 | RETURN (null WHERE x)\n"
                "       |              ^^^^^\n"
                "-------* syntax error, unexpected WHERE");

    expectParse("RETURN (true)", {query({}, returning({parenthesized(boolean(true))}))});

    expectParse("RETURN (-1, 2)", {query({}, returning({literal(embedding({-1.0f, 2.0f}))}))});

    expectParse("RETURN (1, 2.5, -3)", {query({}, returning({literal(embedding({1.0f, 2.5f, -3.0f}))}))});

    expectParse("RETURN (1)", {query({}, returning({parenthesized(integer(1))}))});

    expectParse("RETURN (-1)", {query({}, returning({parenthesized(integer(-1))}))});

    expectParse("RETURN (- 1, 2)", {query({}, returning({literal(embedding({-1.0f, 2.0f}))}))});

    expectError("RETURN (1 + 2, 3)",
                "     1 | RETURN (1 + 2, 3)\n"
                "       |              ^\n"
                "-------* syntax error, unexpected COMMA");

    expectParse("RETURN ()", {query({}, returning({patternPredicate(pattern({path({node({})})}))}))});

    expectParse("RETURN ()-->()",
                {query({},
                       returning({patternPredicate(pattern({path({node({}),
                                                                  edge({._direction = EdgePattern::Direction::Forward}),
                                                                  node({})})}))}))});

    {
        SymbolExpr* filter = symbol("x");

        expectParse("RETURN (WHERE x)",
                    {query({},
                           returning({patternPredicate(pattern({path({node({._where = where(filter)})})},
                                                               where(filter)))}))});
    }

    expectParse("RETURN (:A)",
                {query({}, returning({patternPredicate(pattern({path({node({._labels = labels({"A"})})})}))}))});

    expectParse("RETURN (IS A)",
                {query({}, returning({patternPredicate(pattern({path({node({._labels = labels({"A"})})})}))}))});

    expectParse("RETURN (is AND x)",
                {query({}, returning({parenthesized(logical(LogicalOperator::And, {symbol("is"), symbol("x")}))}))});

    expectParse("RETURN (where + 1)",
                {query({}, returning({parenthesized(binary(BinaryOperator::Add, symbol("where"), integer(1)))}))});

    expectParse("RETURN (where(1))", {query({}, returning({parenthesized(invocation({"where"}, {integer(1)}))}))});

    expectParse("RETURN (where {.x})",
                {query({}, returning({parenthesized(mapProjection("where", {{._key = name("x")}}))}))});

    {
        LiteralExpr* filter = boolean(true);

        expectParse("RETURN (where WHERE true)",
                    {query({},
                           returning({patternPredicate(pattern({path({node({._symbol = "where", ._where = where(filter)})})},
                                                               where(filter)))}))});
    }

    expectError("RETURN (n {a: 1, b: 2",
                "     1 | RETURN (n {a: 1, b: 2\n"
                "       |                     ^\n"
                "-------* syntax error, unexpected PROG_END");

    expectError("RETURN (n:A|B",
                "     1 | RETURN (n:A|B\n"
                "       |             ^\n"
                "-------* syntax error, unexpected PROG_END");

    expectError("RETURN (n:(A|B",
                "     1 | RETURN (n:(A|B\n"
                "       |              ^\n"
                "-------* syntax error, unexpected PROG_END, expecting CPAREN or COLON or PIPE or AMPERSAND");

    {
        SymbolExpr* filter = symbol("x");

        expectParse("RETURN ({a: (1)} WHERE x)",
                    {query({},
                           returning({patternPredicate(pattern({path({node({._properties = properties({{"a", parenthesized(integer(1))}}),
                                                                            ._where = where(filter)})})},
                                                               where(filter)))}))});
    }

    expectParse("RETURN (x).name", {query({}, returning({property({"x", "name"})}))});

    expectParse("RETURN (1).name", {query({}, returning({lookup(parenthesized(integer(1)), "name")}))});

    expectParse("RETURN (x.y).name", {query({}, returning({lookup(parenthesized(property({"x", "y"})), "name")}))});

    expectParse("RETURN ((x)).name", {query({}, returning({property({"x", "name"})}))});
}

TEST_F(ParseEdgeCaseTest, parsesLabelExpressionsAndComprehensions) {
    expectParse("RETURN n:A[0]", {query({}, returning({index(entityType("n", labels({"A"})), integer(0))}))});

    expectError("RETURN n:A.x",
                "     1 | RETURN n:A.x\n"
                "       |           ^\n"
                "-------* syntax error, unexpected DOT");

    expectParse("RETURN n.x:A", {query({}, returning({labelTest(property({"n", "x"}), labels({"A"}))}))});

    expectParse("RETURN n[0]:A", {query({}, returning({labelTest(index(symbol("n"), integer(0)), labels({"A"}))}))});

    expectParse("RETURN n.x.y[1..2][..3][4..][..]",
                {query({},
                       returning({slice(slice(slice(slice(lookup(property({"n", "x"}), "y"), integer(1), integer(2)),
                                                    nullptr,
                                                    integer(3)),
                                              integer(4),
                                              nullptr),
                                        nullptr,
                                        nullptr)}))});

    expectParse("RETURN n.address.city", {query({}, returning({lookup(property({"n", "address"}), "city")}))});

    expectParse("RETURN apoc.coll.sum(xs)", {query({}, returning({invocation({"apoc", "coll", "sum"}, {symbol("xs")})}))});

    expectParse("RETURN [x IN [1] WHERE x:A|B]",
                {query({},
                       returning({listComprehension("x",
                                                    list({integer(1)}),
                                                    entityType("x", labels({"A"})),
                                                    symbol("B"))}))});

    expectParse("RETURN [n:A|B]",
                {query({}, returning({list({entityType("n", labelOr({labels({"A"}), labels({"B"})}))})}))});

    expectParse("RETURN [1, n:A|B]",
                {query({}, returning({list({integer(1), entityType("n", labelOr({labels({"A"}), labels({"B"})}))})}))});

    expectParse("RETURN [(a:A|B)-->(b) | b]",
                {query({},
                       returning({patternComprehension(match({._pattern = pattern({path({node({._symbol = "a",
                                                                                               ._labels = labelOr({labels({"A"}),
                                                                                                                   labels({"B"})})}),
                                                                                         edge({._direction = EdgePattern::Direction::Forward}),
                                                                                         node({._symbol = "b"})})})}),
                                                       symbol("b"))}))});

    expectParse("MATCH (n:A|B) RETURN n",
                {query({match({._pattern = pattern({path({node({._symbol = "n",
                                                                ._labels = labelOr({labels({"A"}), labels({"B"})})})})})})},
                       returning({symbol("n")}))});

    expectParse("RETURN [x IN [1] | x]",
                {query({}, returning({listComprehension("x", list({integer(1)}), nullptr, symbol("x"))}))});

    expectParse("RETURN [x IN [1]]", {query({}, returning({listComprehension("x", list({integer(1)}))}))});

    expectError("RETURN [1 | 2]",
                "     1 | RETURN [1 | 2]\n"
                "       |         ^\n"
                "-------* Invalid comprehension. Expected 'variable IN list', or a path '(...)-[...]-(...)' followed by '|'");

    expectParse("RETURN []", {query({}, returning({list({})}))});

    expectError("RETURN [1, 2",
                "     1 | RETURN [1, 2\n"
                "       |            ^\n"
                "-------* syntax error, unexpected PROG_END");

    expectParse("RETURN [p = (a)-->(b) | p]",
                {query({},
                       returning({patternComprehension(match({._pattern = pattern({namedPath("p",
                                                                                             {node({._symbol = "a"}),
                                                                                              edge({._direction = EdgePattern::Direction::Forward}),
                                                                                              node({._symbol = "b"})})})}),
                                                       symbol("p"))}))});

    expectParse("MATCH (n:A&B&!C) RETURN n",
                {query({match({._pattern = pattern({path({node({._symbol = "n",
                                                                ._labels = labelAnd({labels({"A", "B"}),
                                                                                     labelNot(labels({"C"}))})})})})})},
                       returning({symbol("n")}))});

    expectParse("MATCH (n:!(A|B)) RETURN n",
                {query({match({._pattern = pattern({path({node({._symbol = "n",
                                                                ._labels = labelNot(labelOr({labels({"A"}),
                                                                                             labels({"B"})}))})})})})},
                       returning({symbol("n")}))});
}

TEST_F(ParseEdgeCaseTest, parsesKeywordsUsedAsNames) {
    expectParse("RETURN case AS x", {query({}, returning({named(symbol("case"), "x")}))});

    expectParse("RETURN not AS x", {query({}, returning({named(symbol("not"), "x")}))});

    expectParse("RETURN distinct AS x", {query({}, returning({named(symbol("distinct"), "x")}))});

    expectParse("RETURN DISTINCT *", {query({}, returning(projection({._items = {}, ._distinct = true, ._all = true})))});

    expectParse("RETURN DISTINCT", {query({}, returning({symbol("DISTINCT")}))});

    expectParse("RETURN distinct", {query({}, returning({symbol("distinct")}))});

    expectError("RETURN count(DISTINCT)",
                "     1 | RETURN count(DISTINCT)\n"
                "       |        ^^^^^^^^^^^^^^^\n"
                "-------* Not implemented: count(DISTINCT)");

    expectError("RETURN toUpper(DISTINCT)",
                "     1 | RETURN toUpper(DISTINCT)\n"
                "       |        ^^^^^^^^^^^^^^^^^\n"
                "-------* DISTINCT requires an argument");

    expectParse("RETURN collect(DISTINCT)", {query({}, returning({invocation({"collect"}, {symbol("DISTINCT")})}))});

    expectError("RETURN count()",
                "     1 | RETURN count()\n"
                "       |        ^^^^^^^\n"
                "-------* Not implemented: count()");

    expectParse("RETURN count(*)", {query({}, returning({invocation({"count"}, {wildcard()})}))});

    expectParse("RETURN count(DISTINCT 1, 2)",
                {query({}, returning({distinct(invocation({"count"}, {integer(1), integer(2)}))}))});

    expectParse("RETURN f(DISTINCT 1)", {query({}, returning({distinct(invocation({"f"}, {integer(1)}))}))});

    expectParse("RETURN collect(DISTINCT as)", {query({}, returning({distinct(invocation({"collect"}, {symbol("as")}))}))});

    expectParse("MATCH (n) WHERE CASE as WHEN 1 THEN true END RETURN n",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})},
                                                   where(caseExpr(symbol("as"),
                                                                  {{._tests = {{._value = integer(1)}},
                                                                    ._then = boolean(true)}})))})},
                       returning({symbol("n")}))});

    expectError("ORDER BY case ASC",
                "     1 | ORDER BY case ASC\n"
                "       | ^^^^^\n"
                "-------* syntax error, unexpected ORDER");

    expectError("MATCH (n) RETURN n ORDER BY case ASC",
                "     1 | MATCH (n) RETURN n ORDER BY case ASC\n"
                "       |                                  ^^^\n"
                "-------* syntax error, unexpected PROG_END");

    expectParse("UNWIND not AS x RETURN x", {query({unwind(symbol("not"), "x")}, returning({symbol("x")}))});

    expectParse("UNWIND case AS x RETURN x", {query({unwind(symbol("case"), "x")}, returning({symbol("x")}))});

    expectError("RETURN (case AS x)",
                "     1 | RETURN (case AS x)\n"
                "       |                 ^\n"
                "-------* syntax error, unexpected ID");

    expectParse("RETURN exists", {query({}, returning({symbol("exists")}))});
}

TEST_F(ParseEdgeCaseTest, parsesEntityDetailsAndReadingClauses) {
    expectError("LOAD CSV",
                "     1 | LOAD CSV\n"
                "       |      ^^^\n"
                "-------* syntax error, unexpected PROG_END, expecting STRING_LITERAL");

    expectParse("LOAD CSV 'f' AS r RETURN r", {query({loadCSV({._path = "f", ._alias = "r"})}, returning({symbol("r")}))});

    expectParse("LOAD CSV 'f' WITH HEADERS ON ERROR SKIP AS r RETURN r",
                {query({loadCSV({._path = "f", ._alias = "r", ._headers = true, ._skipOnError = true})},
                       returning({symbol("r")}))});

    expectError("LOAD CSV 'f' ON ERROR 5",
                "     1 | LOAD CSV 'f' ON ERROR 5\n"
                "       |                       ^\n"
                "-------* syntax error, unexpected DIGIT, expecting SKIP or FAIL");

    {
        LiteralExpr* filter = boolean(true);

        expectParse("MATCH (WHERE true) RETURN 1",
                    {query({match({._pattern = pattern({path({node({._where = where(filter)})})}, where(filter))})},
                           returning({integer(1)}))});
    }

    expectParse("MATCH (where {a: 1}) RETURN where",
                {query({match({._pattern = pattern({path({node({._symbol = "where",
                                                                ._properties = properties({{"a", integer(1)}})})})})})},
                       returning({symbol("where")}))});

    expectParse("MATCH (IS AND) RETURN 1",
                {query({match({._pattern = pattern({path({node({._labels = labels({"AND"})})})})})},
                       returning({integer(1)}))});

    expectParse("MATCH (is:Person) RETURN is",
                {query({match({._pattern = pattern({path({node({._symbol = "is", ._labels = labels({"Person"})})})})})},
                       returning({symbol("is")}))});

    expectParse("MATCH (where) RETURN where",
                {query({match({._pattern = pattern({path({node({._symbol = "where"})})})})},
                       returning({symbol("where")}))});

    expectParse("MATCH (is) RETURN is",
                {query({match({._pattern = pattern({path({node({._symbol = "is"})})})})}, returning({symbol("is")}))});

    expectParse("MATCH (is IS A) RETURN is",
                {query({match({._pattern = pattern({path({node({._symbol = "is", ._labels = labels({"A"})})})})})},
                       returning({symbol("is")}))});

    expectParse("MATCH (null) RETURN 1",
                {query({match({._pattern = pattern({path({node({._symbol = "null"})})})})}, returning({integer(1)}))});

    expectParse("MATCH (true:A) RETURN 1",
                {query({match({._pattern = pattern({path({node({._symbol = "true", ._labels = labels({"A"})})})})})},
                       returning({integer(1)}))});

    expectParse("MATCH ()-[where]-() RETURN 1",
                {query({match({._pattern = pattern({path({node({}), edge({._symbol = "where"}), node({})})})})},
                       returning({integer(1)}))});

    {
        LiteralExpr* filter = boolean(true);

        expectParse("MATCH ()-[WHERE true]-() RETURN 1",
                    {query({match({._pattern = pattern({path({node({}), edge({._where = where(filter)}), node({})})},
                                                       where(filter))})},
                           returning({integer(1)}))});
    }

    expectParse("MATCH ()-[IS T]-() RETURN 1",
                {query({match({._pattern = pattern({path({node({}), edge({._labels = labels({"T"})}), node({})})})})},
                       returning({integer(1)}))});

    expectParse("MATCH ()-[is]-() RETURN 1",
                {query({match({._pattern = pattern({path({node({}), edge({._symbol = "is"}), node({})})})})},
                       returning({integer(1)}))});

    expectParse("MATCH ()-[where {a: 1}]-() RETURN 1",
                {query({match({._pattern = pattern({path({node({}),
                                                          edge({._symbol = "where",
                                                                ._properties = properties({{"a", integer(1)}})}),
                                                          node({})})})})},
                       returning({integer(1)}))});

    expectParse("MATCH (n) WITH n SKIP 1 LIMIT 2 WHERE n.x > 1 RETURN n",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})}),
                        with({._projection = projection({._items = {symbol("n")},
                                                         ._skip = skip(integer(1)),
                                                         ._limit = limit(integer(2))}),
                              ._where = where(binary(BinaryOperator::GreaterThan, property({"n", "x"}), integer(1)))})},
                       returning({symbol("n")}))});

    expectError("MATCH (n) RETURN n SKIP 1 LIMIT 2 ORDER BY n",
                "     1 | MATCH (n) RETURN n SKIP 1 LIMIT 2 ORDER BY n\n"
                "       |                                   ^^^^^\n"
                "-------* syntax error, unexpected ORDER");

    expectParse("MATCH (n) RETURN n ORDER BY n.x DESC, n.y ASCENDING SKIP 1",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})})},
                       returning(projection({._items = {symbol("n")},
                                             ._orderBy = orderBy({{property({"n", "x"}), OrderByType::DESC},
                                                                  {property({"n", "y"}), OrderByType::ASC}}),
                                             ._skip = skip(integer(1))})))});

    expectParse("MATCH (n) SHORTESTPATH(a, b, w, d, p) RETURN d",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})}),
                        shortestPath("a", "b", "w", "d", "p")},
                       returning({symbol("d")}))});

    expectParse("MATCH (n) SHORTESTPATH(a, b, null, d, p) RETURN d",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})}),
                        shortestPath("a", "b", "null", "d", "p")},
                       returning({symbol("d")}))});

    expectError("MATCH (n) SHORTESTPATH(a, b, w, d, p) MATCH (m) RETURN d",
                "     1 | MATCH (n) SHORTESTPATH(a, b, w, d, p) MATCH (m) RETURN d\n"
                "       |                                       ^^^^^\n"
                "-------* syntax error, unexpected MATCH, expecting RETURN");

    expectParse("VECTOR SEARCH IN v FOR 3 (1, 2) YIELD ids RETURN ids",
                {query({vectorSearch({._index = "v",
                                      ._neighbours = 3,
                                      ._queryVector = embedding({1.0f, 2.0f}),
                                      ._yield = yield({symbol("ids")})})},
                       returning({symbol("ids")}))});

    expectError("VECTOR SEARCH IN v FOR 3 (1) YIELD ids",
                "     1 | VECTOR SEARCH IN v FOR 3 (1) YIELD ids\n"
                "       |                            ^\n"
                "-------* syntax error, unexpected CPAREN, expecting COMMA");

    expectParse("VECTOR SEARCH IN v FOR 3 (-1.5, 2e3) YIELD *",
                {query({vectorSearch({._index = "v",
                                      ._neighbours = 3,
                                      ._queryVector = embedding({-1.5f, 2000.0f}),
                                      ._yield = yieldAll()})})});

    expectError("MATCH (n IS NULL) RETURN n",
                "     1 | MATCH (n IS NULL) RETURN n\n"
                "       |          ^^\n"
                "-------* syntax error, unexpected IS_VALUE_TEST");

    expectParse("MATCH (n) RETURN n SKIP $s LIMIT $l",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})})},
                       returning(projection({._items = {symbol("n")},
                                             ._skip = skip(parameter("s")),
                                             ._limit = limit(parameter("l"))})))});

    expectParse("MATCH (n) RETURN n ORDER BY n.x DESC, n.y SKIP 1 LIMIT 2",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})})},
                       returning(projection({._items = {symbol("n")},
                                             ._orderBy = orderBy({{property({"n", "x"}), OrderByType::DESC},
                                                                  {property({"n", "y"}), OrderByType::ASC}}),
                                             ._skip = skip(integer(1)),
                                             ._limit = limit(integer(2))})))});

    expectParse("MATCH (n) RETURN n ORDER BY n.x DESCENDING, n.y ASCENDING",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})})},
                       returning(projection({._items = {symbol("n")},
                                             ._orderBy = orderBy({{property({"n", "x"}), OrderByType::DESC},
                                                                  {property({"n", "y"}), OrderByType::ASC}})})))});

    expectParse("MATCH (n) RETURN DISTINCT n.x AS x ORDER BY x ASC",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})})},
                       returning(projection({._items = {named(property({"n", "x"}), "x")},
                                             ._distinct = true,
                                             ._orderBy = orderBy({{symbol("x"), OrderByType::ASC}})})))});

    expectParse("MATCH (n) WITH DISTINCT n ORDER BY n.x SKIP 1 LIMIT 2 WHERE n.x > 0 RETURN n",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})}),
                        with({._projection = projection({._items = {symbol("n")},
                                                         ._distinct = true,
                                                         ._orderBy = orderBy({{property({"n", "x"}), OrderByType::ASC}}),
                                                         ._skip = skip(integer(1)),
                                                         ._limit = limit(integer(2))}),
                              ._where = where(binary(BinaryOperator::GreaterThan, property({"n", "x"}), integer(0)))})},
                       returning({symbol("n")}))});

    expectParse("MATCH (n) ORDER BY n.x LIMIT 3 RETURN n",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})}),
                               ._orderBy = orderBy({{property({"n", "x"}), OrderByType::ASC}}),
                               ._limit = limit(integer(3))})},
                       returning({symbol("n")}))});
}

TEST_F(ParseEdgeCaseTest, parsesLiteralsAndOperators) {
    expectError("RETURN $1, $a, $null, $5x",
                "     1 | RETURN $1, $a, $null, $5x\n"
                "       |                         ^\n"
                "-------* syntax error, unexpected ID");

    expectError("RETURN $",
                "     1 | RETURN $\n"
                "       |        ^\n"
                "-------* syntax error, unexpected PROG_END, expecting a name or DIGIT");

    expectError("RETURN 9223372036854775808",
                "     1 | RETURN 9223372036854775808\n"
                "       |        ^^^^^^^^^^^^^^^^^^^\n"
                "-------* syntax error, unexpected integer literal 9223372036854775808, which exceeds int64 range unless negated");

    expectParse("RETURN -9223372036854775808", {query({}, returning({integer(std::numeric_limits<int64_t>::min())}))});

    expectParse("RETURN --9223372036854775808",
                {query({}, returning({unary(UnaryOperator::Minus, integer(std::numeric_limits<int64_t>::min()))}))});

    expectParse("RETURN - -1", {query({}, returning({integer(1)}))});

    expectParse("RETURN -+-1",
                {query({}, returning({unary(UnaryOperator::Minus, unary(UnaryOperator::Plus, integer(-1)))}))});

    expectParse("RETURN NOT NOT true",
                {query({}, returning({unary(UnaryOperator::Not, unary(UnaryOperator::Not, boolean(true)))}))});

    expectParse("RETURN NOT not", {query({}, returning({unary(UnaryOperator::Not, symbol("not"))}))});

    expectParse("RETURN 1 + not", {query({}, returning({binary(BinaryOperator::Add, integer(1), symbol("not"))}))});

    expectError("RETURN 1 + not x",
                "     1 | RETURN 1 + not x\n"
                "       |                ^\n"
                "-------* syntax error, unexpected ID");

    expectParse("RETURN 1 + case WHEN true THEN 1 END",
                {query({},
                       returning({binary(BinaryOperator::Add,
                                         integer(1),
                                         caseExpr(nullptr,
                                                  {{._tests = {{._value = boolean(true)}}, ._then = integer(1)}}))}))});

    expectError("RETURN 0x1F, 0o17, 1.5e3, .5, 1day",
                "     1 | RETURN 0x1F, 0o17, 1.5e3, .5, 1day\n"
                "       |                                 ^^\n"
                "-------* syntax error, unexpected ID");

    expectError("RETURN 0x8000000000000000",
                "     1 | RETURN 0x8000000000000000\n"
                "       |        ^^^^^^^^^^^^^^^^^^\n"
                "-------* Hexadecimal literal exceeded int64 range.");

    expectError("RETURN 0o1777777777777777777777",
                "     1 | RETURN 0o1777777777777777777777\n"
                "       |        ^^^^^^^^^^^^^^^^^^^^^^^^\n"
                "-------* Octal literal exceeded int64 range.");

    expectParse("RETURN 'a' STARTS WITH 'b' ENDS WITH 'c' CONTAINS 'd'",
                {query({},
                       returning({stringExpr(StringOperator::Contains,
                                             stringExpr(StringOperator::EndsWith,
                                                        stringExpr(StringOperator::StartsWith, string("a"), string("b")),
                                                        string("c")),
                                             string("d"))}))});

    expectParse("RETURN 2 ^ 3 ^ 4 * 5 / 6 % 7 + 8 - 9 || 'a'",
                {query({},
                       returning({binary(BinaryOperator::Concat,
                                         binary(BinaryOperator::Sub,
                                                binary(BinaryOperator::Add,
                                                       binary(BinaryOperator::Mod,
                                                              binary(BinaryOperator::Div,
                                                                     binary(BinaryOperator::Mult,
                                                                            binary(BinaryOperator::Pow,
                                                                                   binary(BinaryOperator::Pow,
                                                                                          integer(2),
                                                                                          integer(3)),
                                                                                   integer(4)),
                                                                            integer(5)),
                                                                     integer(6)),
                                                              integer(7)),
                                                       integer(8)),
                                                integer(9)),
                                         string("a"))}))});

    expectParse("RETURN normalize('a', NFD)",
                {query({}, returning({invocation({"normalize"}, {string("a"), normalForm(UnicodeNormalForm::NFD)})}))});

    expectError("RETURN normalize('a', 5)",
                "     1 | RETURN normalize('a', 5)\n"
                "       |                       ^\n"
                "-------* syntax error, unexpected DIGIT, expecting NFC or NFD or NFKC or NFKD");

    expectError("RETURN normalize('a',)",
                "     1 | RETURN normalize('a',)\n"
                "       |                      ^\n"
                "-------* syntax error, unexpected CPAREN, expecting NFC or NFD or NFKC or NFKD");

    expectParse("RETURN all(x IN [1])",
                {query({},
                       returning({listPredicate(ListPredicateExpr::Quantifier::All,
                                                listComprehension("x", list({integer(1)})))}))});

    expectParse("RETURN any(x IN [1] WHERE x > 0)",
                {query({},
                       returning({listPredicate(ListPredicateExpr::Quantifier::Any,
                                                listComprehension("x",
                                                                  list({integer(1)}),
                                                                  binary(BinaryOperator::GreaterThan,
                                                                         symbol("x"),
                                                                         integer(0))))}))});

    expectParse("RETURN allReduce(a = 0, x IN [1] | a + x, a > 0)",
                {query({},
                       returning({allReduce("a",
                                            integer(0),
                                            "x",
                                            list({integer(1)}),
                                            binary(BinaryOperator::Add, symbol("a"), symbol("x")),
                                            binary(BinaryOperator::GreaterThan, symbol("a"), integer(0)))}))});

    expectParse("RETURN n {.x, .*, y, z: 1}",
                {query({},
                       returning({mapProjection("n",
                                                {{._key = name("x")},
                                                 {._kind = MapProjectionExpr::ItemKind::AllProperties},
                                                 {._kind = MapProjectionExpr::ItemKind::Variable,
                                                  ._key = name("y"),
                                                  ._value = symbol("y")},
                                                 {._kind = MapProjectionExpr::ItemKind::Literal,
                                                  ._key = name("z"),
                                                  ._value = integer(1)}})}))});

    expectParse("RETURN n {}", {query({}, returning({mapProjection("n", {})}))});

    expectError("RETURN n {null}",
                "     1 | RETURN n {null}\n"
                "       |               ^\n"
                "-------* syntax error, unexpected CBRACE, expecting COLON");

    expectError("RETURN n {5}",
                "     1 | RETURN n {5}\n"
                "       |           ^\n"
                "-------* syntax error, unexpected DIGIT, expecting CBRACE or DOT or a name");

    expectParse("RETURN {a: 1, b: {c: [1, {d: 2}]}}",
                {query({},
                       returning({map({{"a", integer(1)},
                                       {"b", map({{"c", list({integer(1), map({{"d", integer(2)}})})}})}})}))});

    expectParse("RETURN {}", {query({}, returning({map({})}))});

    expectParse("MATCH (n) RETURN n.x IN [1] = true <> false",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})})},
                       returning({comparisonChain(binary(BinaryOperator::In, property({"n", "x"}), list({integer(1)})),
                                                  {{BinaryOperator::Equal, boolean(true)},
                                                   {BinaryOperator::NotEqual, boolean(false)}})}))});

    expectError("MATCH (n) RETURN n.x STARTS 5",
                "     1 | MATCH (n) RETURN n.x STARTS 5\n"
                "       |                             ^\n"
                "-------* syntax error, unexpected DIGIT, expecting WITH");

    expectParse("RETURN 1.5, .5, 1e3, -2.5", {query({}, returning({real(1.5), real(0.5), real(1000.0), real(-2.5)}))});

    expectParse("MATCH (n) WHERE n.x = $x RETURN n, $0",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})},
                                                   where(binary(BinaryOperator::Equal,
                                                                property({"n", "x"}),
                                                                parameter("x"))))})},
                       returning({symbol("n"), parameter("0")}))});

    expectParse("RETURN true XOR false",
                {query({}, returning({logical(LogicalOperator::Xor, {boolean(true), boolean(false)})}))});

    expectParse("RETURN true OR false XOR true AND false",
                {query({},
                       returning({logical(LogicalOperator::Or,
                                          {boolean(true),
                                           logical(LogicalOperator::Xor,
                                                   {boolean(false),
                                                    logical(LogicalOperator::And, {boolean(true), boolean(false)})})})}))});

    expectParse("RETURN 1 >= 2",
                {query({}, returning({binary(BinaryOperator::GreaterThanOrEqual, integer(1), integer(2))}))});

    expectParse("RETURN 1 <= 2 >= 0",
                {query({},
                       returning({comparisonChain(integer(1),
                                                  {{BinaryOperator::LessThanOrEqual, integer(2)},
                                                   {BinaryOperator::GreaterThanOrEqual, integer(0)}})}))});
}
