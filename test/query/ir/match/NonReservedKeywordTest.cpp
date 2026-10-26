#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

// Every keyword the lexer knows that openCypher does not reserve
constexpr std::string_view NON_RESERVED_KEYWORDS[] = {
    "any", "available", "call", "change", "collect", "commit", "connect", "cosine",
    "count", "csv", "datetimes", "dimension", "durations", "embedding", "embeddings",
    "error", "euclid", "explain", "extensions", "extract", "fail", "filter", "flat",
    "foreach", "from", "gml", "graph", "graphs", "headers", "hnsw", "if", "index",
    "indexes", "install", "jsonl", "list", "load", "merge_dataparts", "metric", "new",
    "next", "nfc", "nfd", "nfkc", "nfkd", "none", "normalize", "normalized", "parquet",
    "procedures", "pull", "push", "reduce", "s3", "S3", "search", "shortestpath", "show",
    "single", "submit", "type", "vector", "yield",
};

}

class NonReservedKeywordTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(NonReservedKeywordTest, readsAChainEndingInS3) {
    expectRows("MATCH (s1:Person)-[:KNOWS_WELL]->(s2:Person)-[:KNOWS_WELL]->(s3:Person) RETURN s1.name, s2.name, s3.name",
               {{"Adam", "Remy", "Adam"}, {"Remy", "Adam", "Remy"}});
}

TEST_F(NonReservedKeywordTest, namesANodeAfterEveryNonReservedKeyword) {
    for (const std::string_view keyword : NON_RESERVED_KEYWORDS) {
        const std::string variable {keyword};
        const std::string query = "MATCH (" + variable + ":Person) WHERE " + variable + ".age = 32 WITH " + variable + " RETURN " + variable + ".name";

        expectRows(query, {{"Adam"}, {"Remy"}});
    }
}

TEST_F(NonReservedKeywordTest, keepsTheSpellingOfTheKeywordAsTheColumnName) {
    StringRowSink sink;
    runQuery("UNWIND [1, 2] AS S3 RETURN S3", sink);

    EXPECT_EQ(sink.getNames(), std::vector<std::string> {"S3"});
}

TEST_F(NonReservedKeywordTest, namesAnEdgeVector) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[vector:KNOWS_WELL]->(b) RETURN vector.duration", {{"20"}});
}

TEST_F(NonReservedKeywordTest, namesAPathCsv) {
    expectRows("MATCH csv = (a:Person {name: 'Remy'})-[:KNOWS_WELL]->(b) RETURN length(csv)", {{"1"}});
}

TEST_F(NonReservedKeywordTest, unwindsIntoGraph) {
    expectRows("UNWIND [1, 2, 3] AS graph RETURN graph", {{"1"}, {"2"}, {"3"}});
}

TEST_F(NonReservedKeywordTest, readsAnAliasJsonl) {
    expectRows("WITH 1 AS jsonl RETURN jsonl + 1", {{"2"}});
}

TEST_F(NonReservedKeywordTest, iteratesAComprehensionOverS3) {
    expectRows("RETURN [s3 IN [1, 2, 3] WHERE s3 > 1 | s3 * 10]", {{"20, 30"}});
}

TEST_F(NonReservedKeywordTest, reducesIntoCommitOverChange) {
    expectRows("RETURN reduce(commit = 0, change IN [1, 2, 3] | commit + change)", {{"6"}});
}

TEST_F(NonReservedKeywordTest, deletesANodeVector) {
    runWrite("CREATE (:Doomed {name: 'x'})");
    expectRows("MATCH (n:Doomed) RETURN count(n)", {{"1"}});

    runWrite("MATCH (vector:Doomed) DELETE vector");

    expectRows("MATCH (n:Doomed) RETURN count(n)", {{"0"}});
}

TEST_F(NonReservedKeywordTest, testsAVariableNormalizedForItsNormalForm) {
    expectRows("WITH 'a' AS normalized RETURN normalized IS NORMALIZED, normalized IS NOT NFD NORMALIZED", {{"true", "false"}});
}

TEST_F(NonReservedKeywordTest, readsLabelsAndPropertiesNamedAfterKeywords) {
    runWrite("CREATE (:Graph:S3 {graph: 1, vector: 2, s3: 3})");

    expectRows("MATCH (submit:Graph:S3) RETURN submit.graph, submit.vector, submit.s3", {{"1", "2", "3"}});
}
