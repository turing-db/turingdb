#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "iterators/ChunkConfig.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// UNION and UNION ALL end to end over the shared SimpleGraph fixture: two Founders, eight
// Persons and ten Interests, whose names are all distinct, so a branch's rows are told
// from another's by their names alone.
class UnionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink, size_t chunkSize) {
        _interpreter->setChunkSize(chunkSize);

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    // The rows in the order the query emits them: a union concatenates its branches, so
    // which branch a row came through is part of the answer
    void expectRows(std::string_view query, const Rows& expected, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink, chunkSize);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query
                                         << "\nchunk size: " << chunkSize
                                         << "\nactual:\n" << actualText;
    }

    void expectError(std::string_view query, QueryStatus::Status expected, std::string_view message) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink, ChunkConfig::CHUNK_SIZE);

        EXPECT_EQ(status.getStatus(), expected) << "query: " << query;
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    static Rows namedRows(const std::vector<std::string>& names) {
        Rows rows;
        for (const std::string& name : names) {
            rows.push_back(Row {name});
        }

        return rows;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};

    const std::vector<std::string> _founders {"Remy", "Adam"};
    const std::vector<std::string> _people {"Remy", "Adam", "Maxime", "Luc", "Martina", "Suhas", "Cyrus", "Doruk"};
    const std::vector<std::string> _interests {"Computers", "Eighties", "Bio", "Cooking",
                                               "Ghosts", "Padel", "Animals", "Gym", "Travel", "JiuJitsu"};
    const std::vector<size_t> _chunkSizes {1, 2, 3, 7};
};

// The two branches share no row, so UNION and UNION ALL agree: the twelve rows come out
// branch by branch, the Founders first.
TEST_F(UnionTest, concatenatesDisjointBranches) {
    std::vector<std::string> expected = _founders;
    expected.insert(expected.end(), _interests.begin(), _interests.end());

    expectRows("MATCH (a:Founder) RETURN a.name AS name UNION ALL "
               "MATCH (b:Interest) RETURN b.name AS name",
               namedRows(expected));

    expectRows("MATCH (a:Founder) RETURN a.name AS name UNION "
               "MATCH (b:Interest) RETURN b.name AS name",
               namedRows(expected));
}

// Every Founder is a Person, so UNION ALL reports Remy and Adam twice where UNION reports
// them once - and once through the first branch, since the dedup keeps first occurrences.
TEST_F(UnionTest, keepsDuplicatesOnlyUnderUnionAll) {
    std::vector<std::string> withDuplicates = _founders;
    withDuplicates.insert(withDuplicates.end(), _people.begin(), _people.end());

    expectRows("MATCH (a:Founder) RETURN a.name AS name UNION ALL "
               "MATCH (b:Person) RETURN b.name AS name",
               namedRows(withDuplicates));

    expectRows("MATCH (a:Founder) RETURN a.name AS name UNION "
               "MATCH (b:Person) RETURN b.name AS name",
               namedRows(_people));
}

// A branch dedups against the rows of the branches before it, not only against its own:
// the second branch here repeats the first entirely and contributes nothing.
TEST_F(UnionTest, dedupsAcrossBranchesAndNotWithinThem) {
    expectRows("MATCH (a:Founder) RETURN a.name AS name UNION "
               "MATCH (b:Founder) RETURN b.name AS name",
               namedRows(_founders));
}

TEST_F(UnionTest, chainsMoreThanTwoBranches) {
    std::vector<std::string> expected = _founders;
    expected.insert(expected.end(), _interests.begin(), _interests.end());
    expected.insert(expected.end(), _people.begin() + 2, _people.end());

    expectRows("MATCH (a:Founder) RETURN a.name AS name UNION "
               "MATCH (b:Interest) RETURN b.name AS name UNION "
               "MATCH (c:Person) RETURN c.name AS name",
               namedRows(expected));
}

// UNION is left associative, so `A UNION ALL B UNION C` is `(A UNION ALL B) UNION C` and
// its result is distinct(A ++ B ++ C): the last dedup covers every branch before it,
// including the ones an earlier UNION ALL concatenated.
TEST_F(UnionTest, aTrailingUnionDedupsTheBranchesBeforeIt) {
    expectRows("MATCH (a:Founder) RETURN a.name AS name UNION ALL "
               "MATCH (b:Founder) RETURN b.name AS name UNION "
               "MATCH (c:Founder) RETURN c.name AS name",
               namedRows(_founders));
}

// The mirror image: `A UNION B UNION ALL C` dedups A and B and then appends C as it comes,
// so the two Founders are reported twice.
TEST_F(UnionTest, aTrailingUnionAllAppendsWithoutDeduping) {
    std::vector<std::string> expected = _founders;
    expected.insert(expected.end(), _founders.begin(), _founders.end());

    expectRows("MATCH (a:Founder) RETURN a.name AS name UNION "
               "MATCH (b:Founder) RETURN b.name AS name UNION ALL "
               "MATCH (c:Founder) RETURN c.name AS name",
               namedRows(expected));
}

// ORDER BY, SKIP and LIMIT belong to the branch that writes them: each branch is ordered
// and cut on its own, and the union concatenates what is left.
TEST_F(UnionTest, cutsEachBranchOnItsOwnClauses) {
    const Rows expected {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Travel"}, {"Padel"}};

    expectRows("MATCH (a:Person) RETURN a.name AS name ORDER BY a.name LIMIT 3 UNION ALL "
               "MATCH (b:Interest) RETURN b.name AS name ORDER BY b.name DESC LIMIT 2",
               expected);
}

TEST_F(UnionTest, unionsAggregatedBranches) {
    const Rows expected {{"8"}, {"10"}};

    expectRows("MATCH (a:Person) RETURN count(a) AS n UNION ALL "
               "MATCH (b:Interest) RETURN count(b) AS n",
               expected);
}

// A count and a stored integer property are one Cypher INTEGER, so they share a column.
// They reach it as different chunks - a count is an unsigned tally that is never null, a
// property a nullable signed value - and the column is the nullable signed one.
TEST_F(UnionTest, unionsACountWithAnIntegerProperty) {
    const Rows expected {{"8"}, {"32"}, {"32"}};

    expectRows("MATCH (a:Person) RETURN count(a) AS n UNION ALL "
               "MATCH (b:Founder) RETURN b.age AS n",
               expected);
}

TEST_F(UnionTest, unionsAnIntegerPropertyWithACount) {
    const Rows expected {{"32"}, {"32"}, {"8"}};

    expectRows("MATCH (a:Founder) RETURN a.age AS n UNION ALL "
               "MATCH (b:Person) RETURN count(b) AS n",
               expected);
}

// A count and an integer property are one Cypher INTEGER. There are ten Interests and one
// ten-minute edge, so the union reports 10 once, not twice.
TEST_F(UnionTest, dedupsACountAgainstAnIntegerProperty) {
    const Rows expected {{"10"}};

    expectRows("MATCH (a:Interest) RETURN count(a) AS n UNION "
               "MATCH ()-[e]->() WHERE e.duration = 10 RETURN e.duration AS n",
               expected);
}

// One branch owns the characters of its strings and the other borrows them from the graph.
// Both are one Cypher STRING, so the result carries them as one column.
TEST_F(UnionTest, unionsAnEdgeTypeWithAStringProperty) {
    const Rows expected {{"KNOWS_WELL"}, {"INTERESTED_IN"}, {"Remy"}, {"Adam"}};

    expectRows("MATCH ()-[e]->() RETURN type(e) AS v UNION "
               "MATCH (a:Founder) RETURN a.name AS v",
               expected);
}

TEST_F(UnionTest, unionsAStringPropertyWithAnEdgeType) {
    const Rows expected {{"Remy"}, {"Adam"}, {"KNOWS_WELL"}, {"INTERESTED_IN"}};

    expectRows("MATCH (a:Founder) RETURN a.name AS v UNION "
               "MATCH ()-[e]->() RETURN type(e) AS v",
               expected);
}

TEST_F(UnionTest, unionsTraversedBranches) {
    const Rows expected {{"Adam"}, {"Ghosts"}, {"Computers"}, {"Eighties"}, {"Remy"}, {"Bio"}, {"Cooking"}};

    expectRows("MATCH (a:Founder)-[e]->(b) RETURN b.name AS name UNION "
               "MATCH (c:Founder) RETURN c.name AS name",
               expected);
}

TEST_F(UnionTest, unionsUnwoundBranches) {
    const Rows expected {{"1"}, {"2"}, {"3"}, {"4"}};

    expectRows("UNWIND [1, 2, 3] AS x RETURN x AS v UNION "
               "UNWIND [3, 4] AS y RETURN y AS v",
               expected);
}

// A projected constant holds one value standing for every row, and the union still has to
// tell one branch's rows from another's by it: the tag is part of the dedup key, so the
// same name under two tags is two rows and under one tag is one.
TEST_F(UnionTest, dedupsOnAConstantColumnToo) {
    const Rows twoTags {{"Remy", "1"}, {"Adam", "1"}, {"Remy", "2"}, {"Adam", "2"}};
    const Rows oneTag {{"Remy", "1"}, {"Adam", "1"}};

    expectRows("MATCH (a:Founder) RETURN a.name AS name, 1 AS tag UNION "
               "MATCH (b:Founder) RETURN b.name AS name, 2 AS tag",
               twoTags);

    expectRows("MATCH (a:Founder) RETURN a.name AS name, 1 AS tag UNION "
               "MATCH (b:Founder) RETURN b.name AS name, 1 AS tag",
               oneTag);
}

// A projection of constants alone is one row repeated once per matched row, so a dedup
// leaves one of them - and a branch matching nothing leaves none.
TEST_F(UnionTest, dedupsAProjectionOfConstantsAlone) {
    expectRows("RETURN 1 AS x UNION RETURN 1 AS x", Rows {{"1"}});
    expectRows("RETURN 1 AS x UNION RETURN 2 AS x", Rows {{"1"}, {"2"}});
    expectRows("MATCH (n) RETURN 1 AS x UNION MATCH (m) RETURN 2 AS x", Rows {{"1"}, {"2"}});
    expectRows("MATCH (n:NoSuchLabel) RETURN 1 AS x UNION MATCH (m:Founder) RETURN 2 AS x", Rows {{"2"}});
}

// A branch reports no row at all rather than dropping out of the result table
TEST_F(UnionTest, keepsTheResultOfAnEmptyBranch) {
    expectRows("MATCH (a:NoSuchLabel) RETURN a.name AS name UNION ALL "
               "MATCH (b:Founder) RETURN b.name AS name",
               namedRows(_founders));

    expectRows("MATCH (a:Founder) RETURN a.name AS name UNION ALL "
               "MATCH (b:NoSuchLabel) RETURN b.name AS name",
               namedRows(_founders));

    expectRows("MATCH (a:NoSuchLabel) RETURN a.name AS name UNION "
               "MATCH (b:NoSuchLabel) RETURN b.name AS name",
               Rows {});
}

// The seen-set spans the whole run, so a value first seen in one branch's last chunk is
// still recognised in the next branch's first
TEST_F(UnionTest, dedupsAcrossChunkBoundaries) {
    for (const size_t chunkSize : _chunkSizes) {
        expectRows("MATCH (a:Founder) RETURN a.name AS name UNION "
                   "MATCH (b:Person) RETURN b.name AS name",
                   namedRows(_people),
                   chunkSize);
    }
}

TEST_F(UnionTest, concatenatesAcrossChunkBoundaries) {
    std::vector<std::string> expected = _founders;
    expected.insert(expected.end(), _interests.begin(), _interests.end());

    for (const size_t chunkSize : _chunkSizes) {
        expectRows("MATCH (a:Founder) RETURN a.name AS name UNION ALL "
                   "MATCH (b:Interest) RETURN b.name AS name",
                   namedRows(expected),
                   chunkSize);
    }
}

// The union emits one result table, so its branches have to agree on what fills it
TEST_F(UnionTest, rejectsBranchesThatProjectOtherColumns) {
    expectError("MATCH (a:Founder) RETURN a.name AS name UNION "
                "MATCH (b:Interest) RETURN b.name AS other",
                QueryStatus::Status::ANALYZE_ERROR,
                "must return the same column names");

    expectError("MATCH (a:Founder) RETURN a.name AS name UNION "
                "MATCH (b:Interest) RETURN b.name AS name, b.name AS x",
                QueryStatus::Status::ANALYZE_ERROR,
                "must return the same number of columns");

    expectError("MATCH (a:Founder) SET a.age = 1 UNION "
                "MATCH (b:Interest) RETURN b.name AS name",
                QueryStatus::Status::ANALYZE_ERROR,
                "must end with a RETURN clause");
}

// An item with no alias is named as it was written, so two branches spelling their
// variables differently name different columns
TEST_F(UnionTest, rejectsBranchesNamingTheirItemsDifferently) {
    expectError("MATCH (a:Founder) RETURN a.name UNION MATCH (b:Interest) RETURN b.name",
                QueryStatus::Status::ANALYZE_ERROR,
                "must return the same column names");

    expectRows("MATCH (a:Founder) RETURN a.name UNION MATCH (a:Interest) RETURN a.name",
               namedRows({"Remy", "Adam", "Computers", "Eighties", "Bio", "Cooking",
                          "Ghosts", "Padel", "Animals", "Gym", "Travel", "JiuJitsu"}));
}

// A result column carries one value type for the whole table, so branches resolving a
// column to different types are rejected rather than emitted under a header naming one
// of them. The types are only known once the property names are resolved, so the
// rejection comes from lowering and not from the analyzer.
TEST_F(UnionTest, rejectsBranchesWhoseColumnTypesDisagree) {
    expectError("MATCH (a:Person) RETURN a.name AS v UNION ALL MATCH (b:Person) RETURN b.age AS v",
                QueryStatus::Status::EXEC_ERROR,
                "'v' is Int64 in this sub-query and String in the first");

    expectError("MATCH (a:Person) RETURN a AS v UNION ALL MATCH (b:Person) RETURN b.name AS v",
                QueryStatus::Status::EXEC_ERROR,
                "'v' is String in this sub-query and Node in the first");
}

// A literal and a stored property of the same type do agree: the literal is laid out over
// the branch's rows, which gives it the shape a property read has
TEST_F(UnionTest, unionsALiteralWithAStoredProperty) {
    std::vector<std::string> expected = _people;
    expected.push_back("Nobody");

    expectRows("MATCH (a:Person) RETURN a.name AS name UNION ALL RETURN 'Nobody' AS name",
               namedRows(expected));
}

// Each branch dedups its own projection before the union dedups the result, and the two
// together report each name once
TEST_F(UnionTest, unionsBranchesThatAreThemselvesDistinct) {
    expectRows("MATCH (a:Person) RETURN DISTINCT a.name AS name UNION "
               "MATCH (b:Founder) RETURN DISTINCT b.name AS name",
               namedRows(_people));
}

// A branch is a query body in its own right, so whatever a standalone query may be built
// from a branch may be too: a barrier, an optional pattern, a disconnected one
TEST_F(UnionTest, unionsBranchesBuiltFromAnyClause) {
    expectRows("MATCH (a:Person) WITH a WHERE a.isFrench = true RETURN a.name AS name UNION "
               "MATCH (b:Founder) RETURN b.name AS name",
               namedRows({"Remy", "Adam", "Maxime", "Luc"}));

    // Remy and Adam know each other well and nobody else does, so the optional hop leaves
    // the two of them as each other's neighbour
    expectRows("MATCH (a:Founder) OPTIONAL MATCH (a)-[:KNOWS_WELL]->(b) RETURN b.name AS name UNION "
               "MATCH (c:Founder) RETURN c.name AS name",
               namedRows({"Adam", "Remy"}));

    // The product pairs each Founder with each Founder, so the branch matches four rows
    // and the dedup leaves the two names they carry
    expectRows("MATCH (a:Founder), (b:Founder) RETURN a.name AS name UNION "
               "MATCH (c:Founder) RETURN c.name AS name",
               namedRows(_founders));
}

// RETURN * names the variables in scope, so two branches binding the same name project
// the same column and two binding different names project different ones
TEST_F(UnionTest, unionsWildcardProjectionsBindingOneName) {
    expectError("MATCH (a:Founder) RETURN * UNION MATCH (b:Founder) RETURN *",
                QueryStatus::Status::ANALYZE_ERROR,
                "must return the same column names");

    expectRows("MATCH (a:Founder) RETURN * UNION MATCH (a:Interest) RETURN *",
               Rows {{"0"}, {"1"}, {"2"}, {"3"}, {"4"}, {"5"},
                     {"6"}, {"7"}, {"10"}, {"13"}, {"14"}, {"16"}});
}
