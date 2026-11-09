#include <gtest/gtest.h>

#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "NLOutputSink.h"
#include "ParameterMap.h"
#include "ParameterValue.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A null written for a property no datapart holds a value of. simpledb carries no
// embedding, so the property e is new to the graph: the null is the absence of a value
// and needs no dimension, and the entity is submitted without e.
class SetNullOnNewPropertyTest : public WriteQueryTest {
protected:
    void runInChange(std::string_view query, const ChangeID& changeID, const ParameterMap* parameters, QueryStatus& status) {
        NullSink sink;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), changeID, &sink, parameters);
    }

    void applyWrites(std::initializer_list<std::string_view> queries, const ParameterMap* parameters = nullptr) {
        ChangeID changeID;
        openChange(changeID);

        for (const std::string_view query : queries) {
            QueryStatus status;
            runInChange(query, changeID, parameters, status);
            ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
        }

        QueryStatus submitStatus;
        runInChange("CHANGE SUBMIT", changeID, nullptr, submitStatus);
        ASSERT_TRUE(submitStatus.isOk()) << "error: " << submitStatus.getError();
    }

    void expectSubmitRejected(std::string_view query, std::string_view reason) {
        ChangeID changeID;
        openChange(changeID);

        QueryStatus status;
        runInChange(query, changeID, nullptr, status);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        QueryStatus submitStatus;
        runInChange("CHANGE SUBMIT", changeID, nullptr, submitStatus);
        expectFailure(submitStatus, query, reason);

        QueryStatus deleteStatus;
        runInChange("CHANGE DELETE", changeID, nullptr, deleteStatus);
        ASSERT_TRUE(deleteStatus.isOk()) << "error: " << deleteStatus.getError();
    }
};

TEST_F(SetNullOnNewPropertyTest, createsTheNodeWithoutTheEmbeddingItsStatementSetsToNull) {
    applyWrites({"CREATE (n:Vec {name: 'c', e: (1.0, 0.0)}) SET n.e = null"});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e IS NULL, 'e' IN keys(n)", {{"c", "true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, setsToNullTheEmbeddingACommittedStatementCreated) {
    applyWrites({"CREATE (n:Vec {name: 'c', e: (1.0, 0.0)})",
                 "COMMIT",
                 "MATCH (n:Vec {name: 'c'}) SET n.e = null"});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e IS NULL, 'e' IN keys(n)", {{"c", "true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, setsToNullAfterACommitTheEmbeddingNoCommittedNodeHolds) {
    applyWrites({"CREATE (n:Vec {name: 'c', e: (1.0, 0.0)}) SET n.e = null",
                 "COMMIT",
                 "MATCH (n:Vec {name: 'c'}) SET n.e = null"});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e IS NULL, 'e' IN keys(n)", {{"c", "true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, setsToNullOnACommittedNodeAnEmbeddingOnlyAPendingNodeHolds) {
    applyWrites({"CREATE (n:Vec {name: 'c', e: (1.0, 0.0)})",
                 "MATCH (p:Person {name: 'Remy'}) SET p.e = null"});

    expectRows("MATCH (n) WHERE n.e IS NOT NULL RETURN n.name, euclidean_distance(n.e, (1.0, 0.0))", {{"c", "0.000000"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.e IS NULL, 'e' IN keys(p)", {{"true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, setsToNullOnACommittedNodeAnEmbeddingNoNodeKeeps) {
    applyWrites({"CREATE (n:Vec {name: 'c', e: (1.0, 0.0)}) SET n.e = null",
                 "MATCH (p:Person {name: 'Remy'}) SET p.e = null"});

    expectRows("MATCH (n) WHERE n.e IS NOT NULL RETURN n.name", {});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.e IS NULL, 'e' IN keys(p)", {{"true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, setsToNullTheEmbeddingAnEarlierStatementSetOnAnotherNode) {
    applyWrites({"MATCH (p:Person {name: 'Remy'}) SET p.e = (1.0, 0.0)",
                 "MATCH (p:Person {name: 'Adam'}) SET p.e = null"});

    expectRows("MATCH (n) WHERE n.e IS NOT NULL RETURN n.name, euclidean_distance(n.e, (1.0, 0.0))", {{"Remy", "0.000000"}});
    expectRows("MATCH (p:Person {name: 'Adam'}) RETURN p.e IS NULL, 'e' IN keys(p)", {{"true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, keepsTheNullAStatementSetsAfterAnEmbeddingOnTheSameNode) {
    applyWrites({"MATCH (p:Person {name: 'Remy'}) SET p.e = (1.0, 0.0)",
                 "MATCH (p:Person {name: 'Remy'}) SET p.e = null"});

    expectRows("MATCH (n) WHERE n.e IS NOT NULL RETURN n.name", {});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.e IS NULL, 'e' IN keys(p)", {{"true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, keepsTheNullAStatementSetsAfterAnEmbeddingOnTheSameEdge) {
    applyWrites({"MATCH ()-[r:KNOWS_WELL {name: 'Remy -> Adam'}]->() SET r.e = (1.0, 0.0)",
                 "MATCH ()-[r:KNOWS_WELL {name: 'Remy -> Adam'}]->() SET r.e = null"});

    expectRows("MATCH ()-[r]->() WHERE r.e IS NOT NULL RETURN r.name", {});
    expectRows("MATCH ()-[r:KNOWS_WELL {name: 'Remy -> Adam'}]->() RETURN r.e IS NULL, 'e' IN keys(r)", {{"true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, createsTheNodeWithoutTheEmbeddingAMapAddsAsNull) {
    applyWrites({"CREATE (n:Vec {name: 'c', e: (1.0, 0.0)}) SET n += {e: null}"});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e IS NULL, 'e' IN keys(n)", {{"c", "true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, createsTheNodeWithoutTheEmbeddingAMapReplacesWithNull) {
    applyWrites({"CREATE (n:Vec {name: 'c', e: (1.0, 0.0)}) SET n = {name: 'c', e: null}"});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e IS NULL, 'e' IN keys(n)", {{"c", "true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, createsTheNodeWithoutTheEmbeddingItsStatementRemoves) {
    applyWrites({"CREATE (n:Vec {name: 'c', e: (1.0, 0.0)}) REMOVE n.e"});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e IS NULL, 'e' IN keys(n)", {{"c", "true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, createsWithANullTheEmbeddingALaterNodeOfTheStatementHolds) {
    applyWrites({"CREATE (b:Vec {name: 'b', e: null}), (a:Vec {name: 'a', e: (1.0, 0.0)})"});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e IS NULL, 'e' IN keys(n)",
               {{"a", "false", "true"}, {"b", "true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, createsWithANullAnEmbeddingAnEarlierStatementRemoved) {
    applyWrites({"CREATE (n:Vec {name: 'a', e: (1.0, 0.0)}) SET n.e = null",
                 "CREATE (n:Vec {name: 'b', e: null})"});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e IS NULL, 'e' IN keys(n)", {{"a", "true", "false"}, {"b", "true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, createsTheEdgeWithoutTheEmbeddingItsStatementSetsToNull) {
    applyWrites({"CREATE (a:Vec {name: 'a'})-[r:R {name: 'r', e: (1.0, 0.0)}]->(b:Vec {name: 'b'}) SET r.e = null"});

    expectRows("MATCH ()-[r:R]->() RETURN r.name, r.e IS NULL, 'e' IN keys(r)", {{"r", "true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, setsToNullOnACommittedEdgeAnEmbeddingOnlyAPendingEdgeHolds) {
    applyWrites({"CREATE (a:Vec {name: 'a'})-[r:R {name: 'r', e: (1.0, 0.0)}]->(b:Vec {name: 'b'})",
                 "MATCH ()-[r:KNOWS_WELL {name: 'Remy -> Adam'}]->() SET r.e = null"});

    expectRows("MATCH ()-[r]->() WHERE r.e IS NOT NULL RETURN r.name, euclidean_distance(r.e, (1.0, 0.0))", {{"r", "0.000000"}});
    expectRows("MATCH ()-[r:KNOWS_WELL {name: 'Remy -> Adam'}]->() RETURN r.e IS NULL, 'e' IN keys(r)", {{"true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, createsTheNodeWithoutTheEmbeddingAMergeSetsToNullOnCreate) {
    applyWrites({"CREATE (n:Vec {name: 'a', e: (1.0, 0.0)}) SET n.e = null",
                 "MERGE (n:Vec {name: 'c'}) ON CREATE SET n.e = null"});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e IS NULL, 'e' IN keys(n)", {{"a", "true", "false"}, {"c", "true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, createsTheNodeWithoutTheEmbeddingANullParameterSets) {
    ParameterMap parameters;
    parameters.insert("v").setNull();

    applyWrites({"CREATE (n:Vec {name: 'c', e: (1.0, 0.0)}) SET n.e = $v"}, &parameters);

    expectRows("MATCH (n:Vec) RETURN n.name, n.e IS NULL, 'e' IN keys(n)", {{"c", "true", "false"}});
}

TEST_F(SetNullOnNewPropertyTest, createsTheNodeWithoutTheValueOfEachTypeItsStatementSetsToNull) {
    const std::vector<std::pair<std::string_view, std::string_view>> properties {
        {"s", "'x'"},
        {"i", "1"},
        {"l", "[1, 2]"},
        {"m", "{a: 1}"},
        {"d", "datetime('2020-01-01T00:00:00Z')"},
        {"u", "duration(5)"},
        {"p", "point({x: 1.0, y: 2.0})"},
    };

    for (const auto& [property, value] : properties) {
        const std::string create = fmt::format("CREATE (n:Typed {{name: '{}', {}: {}}}) SET n.{} = null", property, property, value, property);
        applyWrites({create});

        const std::string read = fmt::format("MATCH (n:Typed {{name: '{}'}}) RETURN n.{} IS NULL, '{}' IN keys(n)", property, property, property);
        expectRows(read, {{"true", "false"}});
    }
}

TEST_F(SetNullOnNewPropertyTest, takesTheDimensionOfTheFirstEmbeddingWrittenAfterTheNull) {
    applyWrites({"CREATE (n:Vec {name: 'c', e: (1.0, 0.0)}) SET n.e = null"});
    applyWrites({"MATCH (n:Vec {name: 'c'}) SET n.e = (1.0, 0.0, 0.0)"});

    expectRows("MATCH (n:Vec) RETURN n.name, euclidean_distance(n.e, (1.0, 0.0, 0.0)), 'e' IN keys(n)", {{"c", "0.000000", "true"}});

    expectSubmitRejected("MATCH (p:Person {name: 'Remy'}) SET p.e = (1.0, 0.0)",
                         "Embedding property 'e' has dimension 3 and cannot take an embedding of dimension 2");
}

TEST_F(SetNullOnNewPropertyTest, mergesTheDatapartsANullOnANewEmbeddingWasWrittenIn) {
    applyWrites({"CREATE (n:Vec {name: 'a', e: (1.0, 0.0)}) SET n.e = null",
                 "MATCH (p:Person {name: 'Remy'}) SET p.e = null"});
    applyWrites({"MATCH (p:Person {name: 'Adam'}) SET p.e = (1.0, 0.0)"});

    QueryStatus mergeStatus;
    runInChange("MERGE_DATAPARTS", ChangeID::head(), nullptr, mergeStatus);
    ASSERT_TRUE(mergeStatus.isOk()) << "error: " << mergeStatus.getError();

    expectRows("MATCH (n) WHERE n.e IS NOT NULL RETURN n.name, euclidean_distance(n.e, (1.0, 0.0))", {{"Adam", "0.000000"}});
    expectRows("MATCH (n) WHERE n.name IN ['a', 'Remy'] RETURN n.name, 'e' IN keys(n)", {{"Remy", "false"}, {"a", "false"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
