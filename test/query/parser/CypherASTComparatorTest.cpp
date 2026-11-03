#include <gtest/gtest.h>

#include <stdint.h>
#include <string>
#include <string_view>
#include <vector>

#include "CypherAST.h"
#include "CypherASTComparator.h"
#include "CypherCorpus.h"
#include "CypherParser.h"
#include "Literal.h"
#include "ParserException.h"
#include "Projection.h"
#include "SinglePartQuery.h"
#include "expr/BinaryExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/LogicalExpr.h"
#include "stmt/ReturnStmt.h"

using namespace db;
using namespace turing::test;

namespace {

struct DifferenceCase {
    std::string_view _expected;
    std::string_view _actual;
    std::string_view _difference;
};

// Constructs no query of the fuzzing corpus holds
constexpr std::string_view EXTRA_QUERIES[] = {
    "MATCH (n) RETURN COUNT { MATCH (n)-->(m) RETURN m } AS c, COUNT { (n)-->(m) } AS d",
    "MATCH (n) RETURN COLLECT { MATCH (n)-->(m) RETURN m.name } AS names",
    "MATCH (n:$label) WHERE n.x = $value RETURN n, $0",
    "MATCH (n:A&!B|C) RETURN n",
    "RETURN all(x IN [1, 2] WHERE x > 0), any(x IN [1] WHERE x = 1), none(x IN [] WHERE true), single(x IN [1] WHERE x = 1)",
    "RETURN reduce(total = 0, x IN [1, 2] | total + x), allReduce(total = 0, x IN [1, 2] | total + x, total < 10)",
    "MATCH (n) RETURN n {.name, .*, rank: 1, n}",
    "MATCH ()-[r]->() RETURN startNode(r).name",
    "RETURN normalize('a', NFD), 'a' IS NFKC NORMALIZED, 'a' IS NOT NORMALIZED",
    "RETURN CASE 1 WHEN < 2 THEN 'a' WHEN IS NULL THEN 'b' WHEN IS NOT NULL THEN 'c' WHEN 3, 4 THEN 'd' ELSE 'e' END",
    "MATCH (n) RETURN n.x AS x UNION RETURN 2 AS x NEXT RETURN x",
    "RETURN 1 AS x NEXT RETURN x AS y UNION RETURN 2 AS y",
    "RETURN 1 AS x NEXT WHEN x > 0 THEN RETURN x AS y ELSE RETURN 0 AS y",
    "MATCH (a WHERE a.x = 1)-[r WHERE r.y = 2]->(b) WHERE b.z = 3 RETURN a",
    "MERGE (n:A {x: 1}) ON CREATE SET n.y = 1 ON MATCH SET n.z = 2 ON CREATE SET n.w = 3",
    "CALL db.labels() YIELD label WHERE label = 'A' RETURN label",
    "EXPLAIN (all, before Fold, after [Prune, Fuse], around Hoist) RETURN 1",
    "LOAD JSONL 'g.jsonl' AS g WITH EMBEDDINGS [{emb, 4}, {'vec', 8}] WITH DATETIMES [created] WITH DURATIONS [elapsed]",
    "LOAD PARQUET 'p.parquet' AS g WITH DURATIONS [elapsed]",
    "LOAD GML 'graph.gml' AS g",
    "LOAD COMMIT 'abc123'",
    "INSTALL extension",
    "MERGE_DATAPARTS",
    "CREATE INDEX knowsSince FOR [r:KNOWS] ON r.since",
    "S3 CONNECT 'id' 'secret' 'eu-west-1'",
    "S3 PULL 's3://bucket/key' '/tmp/file'",
    "S3 PUSH '/tmp/file' 's3://bucket/key'",
};

constexpr std::string_view CHAIN_QUERY = "RETURN 1 < 2 < 3";

constexpr std::string_view CHAIN_ITEM = "queries[0] SinglePartQuery > returnStmt > projection > items[0] LogicalExpr";

const DifferenceCase DIFFERENCES[] = {
    {"RETURN 1, 1 + 3",
     "RETURN 1, 1 + 2",
     "queries[0] SinglePartQuery > returnStmt > projection > items[1] BinaryExpr > rhs LiteralExpr > value: expected 3, actual 2"},
    {"RETURN 1 + 2",
     "RETURN 1 - 2",
     "queries[0] SinglePartQuery > returnStmt > projection > items[0] BinaryExpr > operator: expected ADD, actual SUB"},
    {"RETURN 1 + 2",
     "RETURN 2 + 1",
     "queries[0] SinglePartQuery > returnStmt > projection > items[0] BinaryExpr > lhs LiteralExpr > value: expected 1, actual 2"},
    {"RETURN 1 AS one",
     "RETURN 1 AS uno",
     "queries[0] SinglePartQuery > returnStmt > projection > items[0] LiteralExpr > name: expected \"one\", actual \"uno\""},
    {"RETURN (1) + 2",
     "RETURN 1 + 2",
     "queries[0] SinglePartQuery > returnStmt > projection > items[0] BinaryExpr > lhs LiteralExpr > parenthesized: expected true, actual false"},
    {"RETURN 1",
     "RETURN 1.0",
     "queries[0] SinglePartQuery > returnStmt > projection > items[0] LiteralExpr > literal: expected IntegerLiteral, actual DoubleLiteral"},
    {"RETURN {a: 1, b: 2}",
     "RETURN {a: 1, c: 2}",
     "queries[0] SinglePartQuery > returnStmt > projection > items[0] LiteralExpr > entries[1] > key: expected \"b\", actual \"c\""},
    {"RETURN 1 < 2 < 3",
     "RETURN 1 < 2 AND 2 < 3",
     "queries[0] SinglePartQuery > returnStmt > projection > items[0] LogicalExpr > comparisonChain: expected true, actual false"},
    {"RETURN CASE WHEN true THEN 1 ELSE 2 END",
     "RETURN CASE WHEN true THEN 3 ELSE 2 END",
     "queries[0] SinglePartQuery > returnStmt > projection > items[0] CaseExpr > branches[0] > then LiteralExpr > value: expected 1, actual 3"},
    {"MATCH (n) RETURN count(DISTINCT n)",
     "MATCH (n) RETURN count(n)",
     "queries[0] SinglePartQuery > returnStmt > projection > items[0] FunctionInvocationExpr > distinct: expected true, actual false"},
    {"MATCH (n) RETURN n.name",
     "MATCH (n) RETURN n.age",
     "queries[0] SinglePartQuery > returnStmt > projection > items[0] PropertyExpr > fullName[1]: expected \"name\", actual \"age\""},
    {"RETURN DISTINCT 1",
     "RETURN 1",
     "queries[0] SinglePartQuery > returnStmt > projection > distinct: expected true, actual false"},
    {"MATCH (n) RETURN n ORDER BY n.age DESC",
     "MATCH (n) RETURN n ORDER BY n.age ASC",
     "queries[0] SinglePartQuery > returnStmt > projection > orderBy > items[0] > type: expected DESC, actual ASC"},
    {"RETURN 1 SKIP 1",
     "RETURN 1",
     "queries[0] SinglePartQuery > returnStmt > projection > skip: expected Skip, actual null"},
    {"OPTIONAL MATCH (n) RETURN n",
     "MATCH (n) RETURN n",
     "queries[0] SinglePartQuery > stmts[0] MatchStmt > optional: expected true, actual false"},
    {"MATCH (n:Person) RETURN n",
     "MATCH (n:Animal) RETURN n",
     "queries[0] SinglePartQuery > stmts[0] MatchStmt > pattern > elements[0] > entities[0] NodePattern > labelExpr LABELS > names[0]: expected \"Person\", actual \"Animal\""},
    {"MATCH p = (a) RETURN p",
     "MATCH q = (a) RETURN p",
     "queries[0] SinglePartQuery > stmts[0] MatchStmt > pattern > elements[0] > pathSymbol: expected \"p\", actual \"q\""},
    {"MATCH (a)-->(b) RETURN a",
     "MATCH (a)<--(b) RETURN a",
     "queries[0] SinglePartQuery > stmts[0] MatchStmt > pattern > elements[0] > entities[1] EdgePattern > direction: expected Forward, actual Backward"},
    {"MATCH (a)-[*1..3]->(b) RETURN a",
     "MATCH (a)-[*1..4]->(b) RETURN a",
     "queries[0] SinglePartQuery > stmts[0] MatchStmt > pattern > elements[0] > entities[1] EdgePattern > quantifiedPath > rhs: expected 3, actual 4"},
    {"MATCH (a)((x)-[:R]->(y)){1,3}(b) RETURN a",
     "MATCH (a)((x)-[:S]->(y)){1,3}(b) RETURN a",
     "queries[0] SinglePartQuery > stmts[0] MatchStmt > pattern > elements[0] > entities[1] EdgePattern > hops[0] > labelExpr LABELS > names[0]: expected \"R\", actual \"S\""},
    {"MATCH (a) WHERE (a)-->() RETURN a",
     "MATCH (a) WHERE (a)<--() RETURN a",
     "queries[0] SinglePartQuery > stmts[0] MatchStmt > pattern > where > expr ExistsExpr > body SinglePartQuery > stmts[0] MatchStmt > pattern > elements[0] > entities[1] EdgePattern > direction: expected Forward, actual Backward"},
    {"MATCH (n) SET n.x = 1",
     "MATCH (n) SET n += {x: 1}",
     "queries[0] SinglePartQuery > stmts[1] SetStmt > items[0]: expected PropertyExprAssign, actual EntityPropertiesAssign"},
    {"CALL db.labels() YIELD a AS name RETURN name",
     "CALL db.labels() YIELD b AS name RETURN name",
     "queries[0] SinglePartQuery > stmts[0] CallStmt > yield > items[0] > symbol: expected \"a\" AS \"name\", actual \"b\" AS \"name\""},
    {"CALL db.labels() YIELD * RETURN 1",
     "CALL db.labels() YIELD label RETURN 1",
     "queries[0] SinglePartQuery > stmts[0] CallStmt > yield > items: expected null, actual YieldItems"},
    {"CALL db.labels()",
     "CALL db.labels() RETURN 1",
     "queries[0] SinglePartQuery > stmts[0] CallStmt > standaloneCall: expected true, actual false"},
    {"MATCH (a) CALL (a) { RETURN 1 AS x } RETURN x",
     "MATCH (a) CALL (*) { RETURN 1 AS x } RETURN x",
     "queries[0] SinglePartQuery > stmts[1] CallSubqueryStmt > importsEverything: expected false, actual true"},
    {"RETURN 1 AS x NEXT RETURN x",
     "WITH 1 AS x RETURN x",
     "queries[0] SinglePartQuery > stmts[0] WithStmt > segmentReturn: expected true, actual false"},
    {"LOAD CSV 'f.csv' ON ERROR SKIP AS row RETURN row",
     "LOAD CSV 'f.csv' AS row RETURN row",
     "queries[0] SinglePartQuery > stmts[0] LoadCSVStmt > skipOnError: expected true, actual false"},
    {"RETURN 1 AS x UNION ALL RETURN 2 AS x",
     "RETURN 1 AS x UNION RETURN 2 AS x",
     "queries[0] UnionQuery > branches[1] > all: expected true, actual false"},
    {"CREATE GRAPH alpha",
     "CREATE GRAPH beta",
     "queries[0] CreateGraphQuery > graphName: expected \"alpha\", actual \"beta\""},
    {"LOAD JSONL 'g.jsonl' WITH DURATIONS [a, b]",
     "LOAD JSONL 'g.jsonl' WITH DURATIONS [a]",
     "queries[0] LoadJsonlQuery > durationSpecs: expected {\"a\", \"b\"}, actual {\"a\"}"},
    {"RETURN 1; RETURN 2",
     "RETURN 1",
     "queries: expected size 2, actual size 1"},
    {"EXPLAIN RETURN 1",
     "RETURN 1",
     "explainRequest: expected ExplainRequest, actual null"},
    {"EXPLAIN (db) RETURN 1",
     "EXPLAIN (nl) RETURN 1",
     "explainRequest > db: expected true, actual false"},
};


void parseQuery(CypherAST& ast, std::string_view query) {
    CypherParser parser(&ast);
    parser.parse(query);
}

void expectEqualToItsReparse(const CypherAST& expected, std::string_view query) {
    const std::string reparsedQuery(query);
    CypherAST actual(nullptr, reparsedQuery);
    parseQuery(actual, reparsedQuery);

    std::string difference;
    EXPECT_TRUE(CypherASTComparator::compare(expected, actual, difference)) << query << "\n" << difference;
    EXPECT_TRUE(difference.empty()) << query << "\n" << difference;
}

Expr* createInteger(CypherAST& ast, int64_t value) {
    return LiteralExpr::create(&ast, IntegerLiteral::create(&ast, value));
}

void buildComparisonChain(CypherAST& ast, Expr* firstMiddle, Expr* secondMiddle) {
    BinaryExpr* first = BinaryExpr::create(&ast, BinaryOperator::LessThan, createInteger(ast, 1), firstMiddle);
    BinaryExpr* second = BinaryExpr::create(&ast, BinaryOperator::LessThan, secondMiddle, createInteger(ast, 3));

    Projection* projection = Projection::create(&ast);
    projection->addExpr(LogicalExpr::appendComparison(&ast, first, second));

    SinglePartQuery* query = SinglePartQuery::create(&ast);
    query->setReturnStmt(ReturnStmt::create(&ast, projection));
}

void buildSharedChain(CypherAST& ast) {
    Expr* middle = createInteger(ast, 2);
    buildComparisonChain(ast, middle, middle);
}

void buildUnsharedChain(CypherAST& ast) {
    buildComparisonChain(ast, createInteger(ast, 2), createInteger(ast, 2));
}

}

TEST(CypherASTComparatorTest, findsEveryCorpusQueryEqualToItsReparse) {
    std::vector<std::string> queries;
    readCypherCorpus(queries);
    ASSERT_GT(queries.size(), 5000u);

    size_t comparedCount = 0;

    for (const std::string& query : queries) {
        CypherAST expected(nullptr, query);

        try {
            parseQuery(expected, query);
        } catch (const ParserException&) {
            continue;
        }

        expectEqualToItsReparse(expected, query);
        comparedCount++;
    }

    EXPECT_GT(comparedCount, 4000u);
}

TEST(CypherASTComparatorTest, findsEveryConstructTheCorpusLacksEqualToItsReparse) {
    for (const std::string_view query : EXTRA_QUERIES) {
        CypherAST expected(nullptr, query);
        parseQuery(expected, query);

        expectEqualToItsReparse(expected, query);
    }
}

TEST(CypherASTComparatorTest, namesTheFieldTwoQueriesDifferIn) {
    for (const DifferenceCase& difference : DIFFERENCES) {
        CypherAST expected(nullptr, difference._expected);
        parseQuery(expected, difference._expected);

        CypherAST actual(nullptr, difference._actual);
        parseQuery(actual, difference._actual);

        std::string found;
        EXPECT_FALSE(CypherASTComparator::compare(expected, actual, found)) << difference._expected;
        EXPECT_EQ(found, difference._difference) << difference._expected << "\n" << difference._actual;

        std::string reversed;
        EXPECT_FALSE(CypherASTComparator::compare(actual, expected, reversed)) << difference._actual;
    }
}

TEST(CypherASTComparatorTest, findsAHandBuiltChainEqualToItsParse) {
    CypherAST expected(nullptr, "");
    buildSharedChain(expected);

    CypherAST actual(nullptr, CHAIN_QUERY);
    parseQuery(actual, CHAIN_QUERY);

    std::string difference;
    EXPECT_TRUE(CypherASTComparator::compare(expected, actual, difference)) << difference;
}

TEST(CypherASTComparatorTest, reportsAnOperandOnlyTheActualTreeShares) {
    CypherAST expected(nullptr, "");
    buildUnsharedChain(expected);

    CypherAST actual(nullptr, CHAIN_QUERY);
    parseQuery(actual, CHAIN_QUERY);

    std::string expectedDifference(CHAIN_ITEM);
    expectedDifference += " > operands[1] BinaryExpr > lhs: expected a different node from the one at ";
    expectedDifference += CHAIN_ITEM;
    expectedDifference += " > operands[0] BinaryExpr > rhs, actual the same node";

    std::string difference;
    EXPECT_FALSE(CypherASTComparator::compare(expected, actual, difference));
    EXPECT_EQ(difference, expectedDifference);
}

TEST(CypherASTComparatorTest, reportsAnOperandOnlyTheExpectedTreeShares) {
    CypherAST expected(nullptr, CHAIN_QUERY);
    parseQuery(expected, CHAIN_QUERY);

    CypherAST actual(nullptr, "");
    buildUnsharedChain(actual);

    std::string expectedDifference(CHAIN_ITEM);
    expectedDifference += " > operands[1] BinaryExpr > lhs: expected the same node as at ";
    expectedDifference += CHAIN_ITEM;
    expectedDifference += " > operands[0] BinaryExpr > rhs, actual a different node";

    std::string difference;
    EXPECT_FALSE(CypherASTComparator::compare(expected, actual, difference));
    EXPECT_EQ(difference, expectedDifference);
}
