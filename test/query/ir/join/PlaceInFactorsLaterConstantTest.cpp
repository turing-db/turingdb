#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <string_view>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A filter or an unwind over a cross product, reading a constant a WITH binds after the MATCH
// through an op with a region. simpledb has 8 people and 10 interests, so 80 pairs; Remy is
// node 0, and every name is held by one node.
class PlaceInFactorsLaterConstantTest : public CallV3Test {
protected:
    void expectCount(std::string_view query, std::string_view expected) {
        StringRowSink sink;
        runQuery(query, sink);

        ASSERT_EQ(sink.getRows().size(), 1u) << query;
        EXPECT_EQ(sink.getRows().front().front(), expected) << query;
    }

    // place_in_factors leaves no @param op at the top of the function: each one went into a
    // factor of the product, whose ops are indented one level further
    void expectPlaced(std::string_view query, std::string_view op) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (around place_in_factors) ") + std::string(query), sink);

        std::string before;
        std::string after;
        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "before place_in_factors") {
                before = row.back();
            } else if (row.front() == "after place_in_factors") {
                after = row.back();
            }
        }

        EXPECT_EQ(countAtTheTop(before, op), 1u) << before;
        EXPECT_EQ(countAtTheTop(after, op), 0u) << after;
    }

private:
    static size_t countAtTheTop(const std::string& dump, std::string_view op) {
        const std::string topLevel = std::string(" = ") + std::string(op) + "(";

        size_t count = 0;
        std::istringstream lines(dump);
        std::string line;
        while (std::getline(lines, line)) {
            const bool atTheTop = line.starts_with("    %") && !line.starts_with("     ");
            if (atTheTop && line.find(topLevel) != std::string::npos) {
                count++;
            }
        }

        return count;
    }
};

TEST_F(PlaceInFactorsLaterConstantTest, filtersByAListComprehension) {
    const std::string_view query = "MATCH (a:Person), (b:Interest) WITH a, b, [x IN ['Remy'] | x] AS ns WHERE a.name IN ns RETURN count(*)";

    expectCount(query, "10");
    expectPlaced(query, "db.filter");
}

TEST_F(PlaceInFactorsLaterConstantTest, filtersIDsByAListComprehension) {
    const std::string_view query = "MATCH (a:Person), (b:Interest) WITH a, b, [x IN [0] | x] AS ids WHERE id(a) IN ids RETURN count(*)";

    expectCount(query, "10");
    expectPlaced(query, "db.filter");
}

TEST_F(PlaceInFactorsLaterConstantTest, filtersTheRightFactor) {
    const std::string_view query = "MATCH (a:Person), (b:Interest) WITH a, b, [x IN ['Ghosts'] | x] AS ns WHERE b.name IN ns RETURN count(*)";

    expectCount(query, "8");
    expectPlaced(query, "db.filter");
}

TEST_F(PlaceInFactorsLaterConstantTest, filtersByANestedComprehension) {
    const std::string_view query = "MATCH (a:Person), (b:Interest) WITH a, b, [y IN [x IN ['Remy'] | x] | y] AS ns WHERE a.name IN ns RETURN count(*)";

    expectCount(query, "10");
    expectPlaced(query, "db.filter");
}

TEST_F(PlaceInFactorsLaterConstantTest, filtersByAComprehensionReadingAConstantFromAbove) {
    const std::string_view query = "MATCH (a:Person), (b:Interest) WITH a, b, 'Re' AS pre WITH a, b, [x IN ['Remy', 'Adam'] WHERE x STARTS WITH pre | x] AS ns WHERE a.name IN ns RETURN count(*)";

    expectCount(query, "10");
    expectPlaced(query, "db.filter");
}

TEST_F(PlaceInFactorsLaterConstantTest, keepsAComprehensionOverARowWithItsReader) {
    expectCount("MATCH (a:Person), (b:Interest) WITH a, b, [x IN ['Remy'] | x + a.name] AS ns WHERE 'RemyRemy' IN ns RETURN count(*)", "10");
}

TEST_F(PlaceInFactorsLaterConstantTest, filtersByACase) {
    const std::string_view query = "MATCH (a:Person), (b:Interest) WITH a, b, CASE WHEN size(['x']) = 1 THEN 'Remy' ELSE 'Adam' END AS n WHERE a.name = n RETURN count(*)";

    expectCount(query, "10");
    expectPlaced(query, "db.filter");

    expectCount("MATCH (a:Person), (b:Interest) WITH a, b, CASE size(['x']) WHEN 1 THEN 'Remy' ELSE 'Adam' END AS n WHERE a.name = n RETURN count(*)",
                "10");
}

TEST_F(PlaceInFactorsLaterConstantTest, filtersByACoalesce) {
    const std::string_view query = "MATCH (a:Person), (b:Interest) WITH a, b, coalesce(null, 'Remy') AS n WHERE a.name = n RETURN count(*)";

    expectCount(query, "10");
    expectPlaced(query, "db.filter");
}

TEST_F(PlaceInFactorsLaterConstantTest, filtersByAReduce) {
    expectCount("MATCH (a:Person), (b:Interest) WITH a, b, reduce(s = '', x IN ['Re', 'my'] | s + x) AS n WHERE a.name = n RETURN count(*)",
                "10");
}

TEST_F(PlaceInFactorsLaterConstantTest, filtersByAListPredicate) {
    const std::string_view query = "MATCH (a:Person), (b:Interest) WITH a, b, any(x IN [1] WHERE x = 1) AS t WHERE (a.name = 'Remy') = t RETURN count(*)";

    expectCount(query, "10");
    expectPlaced(query, "db.filter");
}

TEST_F(PlaceInFactorsLaterConstantTest, filtersEachFactorByItsOwnConstant) {
    expectCount("MATCH (a:Person), (b:Interest) WITH a, b, [x IN ['Remy'] | x] AS ns, [x IN ['Ghosts', 'Bio'] | x] AS ms WHERE a.name IN ns AND b.name IN ms RETURN count(*)",
                "2");
    expectCount("MATCH (a:Person), (b:Interest) WITH a, b, [x IN ['Remy'] | x] AS ns WHERE a.name IN ns WITH a, b, [x IN ['Ghosts'] | x] AS ms WHERE b.name IN ms RETURN count(*)",
                "1");
}

TEST_F(PlaceInFactorsLaterConstantTest, keepsTheConjunctReadingBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WITH a, b, [x IN ['Remy'] | x] AS ns WHERE a.name = b.name AND a.name IN ns RETURN count(*)", "1");
}

TEST_F(PlaceInFactorsLaterConstantTest, filtersAFactorOfANestedProduct) {
    expectCount("MATCH (a:Person), (b:Interest), (c:Interest) WITH a, b, c, [x IN ['Remy'] | x] AS ns WHERE a.name IN ns RETURN count(*)", "100");
}

TEST_F(PlaceInFactorsLaterConstantTest, filtersInsideACallBody) {
    expectCount("MATCH (p:Person {name: 'Remy'}) CALL (p) { MATCH (a:Person), (b:Interest) WITH a, b, [x IN [p.name] | x] AS ns WHERE a.name IN ns RETURN count(*) AS c } RETURN c",
                "10");
}

TEST_F(PlaceInFactorsLaterConstantTest, unwindsAListOfARowAndAComprehension) {
    const std::string_view query = "MATCH (a:Person), (b:Interest) WITH a, b, [x IN [1] | x] AS l UNWIND [a.name, l] AS y RETURN count(*)";

    expectCount(query, "160");
    expectPlaced(query, "db.unwind");

    expectCount("MATCH (a:Person), (b:Interest) WITH a, b, [x IN [1] | x] AS l UNWIND [b.name, l] AS y RETURN count(*)", "160");
    expectCount("MATCH (a:Person), (b:Interest) WITH a, b, CASE WHEN size(['x']) = 1 THEN 'Remy' ELSE 'Adam' END AS n UNWIND [a.name, n] AS y RETURN count(*)",
                "160");
}
