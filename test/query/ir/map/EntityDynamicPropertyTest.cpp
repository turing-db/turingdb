#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "QueryStatus.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

bool contains(std::string_view text, std::string_view part) {
    return text.find(part) != std::string_view::npos;
}

}

// n[k] and e[k] read the property of a node or an edge a key computed by the query names,
// openCypher's dynamic property access (TCK Graph7)
class EntityDynamicPropertyTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectWriteRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runWrite(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void programOf(std::string_view dialect, std::string_view query, std::string& program) {
        StringRowSink sink;
        runQuery("EXPLAIN (" + std::string(dialect) + ") " + std::string(query), sink);

        ASSERT_EQ(sink.getRows().size(), 1u);
        program = sink.getRows().front().back();
    }
};

TEST_F(EntityDynamicPropertyTest, readsAPropertyOfAMatchedNodeByAComputedKey) {
    runWrite("CREATE (:Thing {name: 'Apa'})");

    expectRows("MATCH (n {name: 'Apa'}) RETURN n['nam' + 'e'] AS value", {{"Apa"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n['nam' + 'e'], n['a' + 'ge']", {{"Remy", "32"}});
}

TEST_F(EntityDynamicPropertyTest, readsAPropertyOfACreatedNodeByAComputedKey) {
    expectWriteRows("CREATE (n:Thing {name: 'Apa'}) RETURN n['nam' + 'e'] AS value", {{"Apa"}});
}

TEST_F(EntityDynamicPropertyTest, readsAPropertyByAKeyAParameterHolds) {
    ParameterMap parameters;

    ParameterValue key;
    key.setString("name");
    parameters.set("idx", key);

    StringRowSink sink;
    runWrite("CREATE (n:Thing {name: 'Apa'}) RETURN n[$idx] AS value", parameters, sink);

    EXPECT_EQ(sink.getRows(), Rows({{"Apa"}}));
}

TEST_F(EntityDynamicPropertyTest, readsAPropertyByAKeyAVariableHolds) {
    expectRows("MATCH (n:Person) WITH n, 'name' AS k RETURN n[k]",
               {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}});

    expectRows("MATCH ()-[e]->() WITH e, 'duration' AS k RETURN e[k]",
               {{"10"}, {"15"}, {"20"}, {"20"}, {"20"}, {"20"}, {"20"}, {"200"},
                {"null"}, {"null"}, {"null"}, {"null"}, {"null"},
                {"null"}, {"null"}, {"null"}, {"null"}, {"null"}});
}

TEST_F(EntityDynamicPropertyTest, plansAKeyAVariableHoldsAsAKeyNamedByTheQuery) {
    std::string program;
    programOf("db", "MATCH (n:Person) WITH n, 'name' AS k RETURN n[k]", program);

    EXPECT_TRUE(contains(program, "db.get_node_properties(%")) << program;
    EXPECT_FALSE(contains(program, "db.static_map_key")) << program;
    EXPECT_FALSE(contains(program, "db.dynamic_map_key")) << program;
}

TEST_F(EntityDynamicPropertyTest, readsADifferentPropertyInEachRow) {
    const std::string_view query = "UNWIND ['name', 'age'] AS k MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] "
                                   "RETURN n.name, k, n[k]";

    expectRows(query, {{"Luc", "age", "null"}, {"Luc", "name", "Luc"}, {"Remy", "age", "32"}, {"Remy", "name", "Remy"}});

    std::string program;
    programOf("db", query, program);
    EXPECT_TRUE(contains(program, "db.dynamic_map_key")) << program;

    expectRows("UNWIND ['name', 'duration', 'proficiency'] AS k MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() "
               "RETURN k, e[k]",
               {{"duration", "20"}, {"name", "Remy -> Adam"}, {"proficiency", "null"}});
}

TEST_F(EntityDynamicPropertyTest, readsNullByANullKey) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n[null]", {{"null"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n, null AS k RETURN n[k]", {{"null"}});
    expectRows("UNWIND ['name', null] AS k MATCH (n:Person {name: 'Remy'}) RETURN n[k]", {{"Remy"}, {"null"}});
    expectRows("MATCH ()-[e:KNOWS_WELL]->() RETURN e[null]", {{"null"}, {"null"}, {"null"}});
}

TEST_F(EntityDynamicPropertyTest, readsNullForAPropertyTheEntityLacks) {
    expectRows("MATCH (n:Person {name: 'Luc'}) RETURN n['a' + 'ge'], n['no' + 'such']", {{"null", "null"}});
    expectRows("MATCH (n:Person {name: 'Luc'}) WITH n, 'age' AS k RETURN n[k]", {{"null"}});
}

TEST_F(EntityDynamicPropertyTest, readsNullOffAnEntityAnOptionalMatchMissed) {
    expectRows("OPTIONAL MATCH (n:Person {name: 'Nobody'}) WITH n, 'name' AS k RETURN n[k]", {{"null"}});
    expectRows("UNWIND ['name', 'age'] AS k OPTIONAL MATCH (n:Person {name: 'Nobody'}) RETURN k, n[k]",
               {{"age", "null"}, {"name", "null"}});
    expectRows("UNWIND [{k: 'name'}] AS m OPTIONAL MATCH (n:Person {name: 'Nobody'}) RETURN n[m.k]", {{"null"}});

    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f:Person) "
               "WITH p, f UNWIND ['name', 'age'] AS k RETURN p.name, k, f[k]",
               {{"Luc", "age", "null"}, {"Luc", "name", "null"}, {"Remy", "age", "32"}, {"Remy", "name", "Adam"}});

    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] OPTIONAL MATCH (p)-[e:KNOWS_WELL]->() "
               "WITH p, e UNWIND ['name', 'duration'] AS k RETURN p.name, k, e[k]",
               {{"Luc", "duration", "null"}, {"Luc", "name", "null"}, {"Remy", "duration", "20"}, {"Remy", "name", "Remy -> Adam"}});
}

TEST_F(EntityDynamicPropertyTest, rejectsAKeyThatIsNoString) {
    runQueryExpectingError("MATCH (n:Person) WHERE n.age > 100 RETURN n[0]",
                           QueryStatus::Status::ANALYZE_ERROR,
                           "A node is indexed by a string key, not by 'Integer'");
    runQueryExpectingError("MATCH (n:Person) WHERE n.age > 100 WITH n, 1 AS k RETURN n[k]",
                           QueryStatus::Status::ANALYZE_ERROR,
                           "A node is indexed by a string key, not by 'Integer'");
    runQueryExpectingError("MATCH ()-[e]->() WHERE e.duration > 1000 RETURN e[0]",
                           QueryStatus::Status::ANALYZE_ERROR,
                           "A relationship is indexed by a string key, not by 'Integer'");
}

TEST_F(EntityDynamicPropertyTest, raisesOnACellKeyHoldingAnInteger) {
    runQueryExpectingError("UNWIND [{k: 1}] AS m MATCH (n:Person {name: 'Remy'}) RETURN n[m.k]",
                           QueryStatus::Status::EXEC_ERROR,
                           "A node is indexed by a string key, not by 'Integer'");
    runQueryExpectingError("UNWIND ['name', 1] AS k MATCH (n:Person {name: 'Remy'}) RETURN n[k]",
                           QueryStatus::Status::EXEC_ERROR,
                           "A node is indexed by a string key, not by 'Integer'");
    runQueryExpectingError("UNWIND [{k: 1}] AS m MATCH ()-[e:KNOWS_WELL]->() RETURN e[m.k]",
                           QueryStatus::Status::EXEC_ERROR,
                           "A relationship is indexed by a string key, not by 'Integer'");
}

TEST_F(EntityDynamicPropertyTest, readsAPropertyByAKeyACellHolds) {
    expectRows("UNWIND [{k: 'name'}, {k: 'age'}, {k: null}] AS m MATCH (n:Person {name: 'Remy'}) RETURN m.k, n[m.k]",
               {{"age", "32"}, {"name", "Remy"}, {"null", "null"}});
    expectRows("UNWIND [{k: 'name'}, {k: 'duration'}] AS m MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() "
               "RETURN m.k, e[m.k]",
               {{"duration", "20"}, {"name", "Remy -> Adam"}});
}

TEST_F(EntityDynamicPropertyTest, readsEveryPropertyItsKeysName) {
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND [k IN keys(n) | n[k]] AS v RETURN v",
               {{"18/01"}, {"32"}, {"Remy"}, {"true"}, {"true"}});
    expectRows("MATCH (:Person {name: 'Remy'})-[e:INTERESTED_IN]->(:Interest {name: 'Ghosts'}) "
               "UNWIND [k IN keys(e) | e[k]] AS v RETURN v",
               {{"20"}, {"Remy -> Ghosts"}, {"expert"}});
}

TEST_F(EntityDynamicPropertyTest, readsWhatTheQueryWrote) {
    expectWriteRows("CREATE (n:Thing {name: 'Apa', rank: 3}) WITH n UNWIND ['name', 'rank'] AS k RETURN k, n[k]",
                    {{"name", "Apa"}, {"rank", "3"}});
    expectWriteRows("MATCH (n:Person {name: 'Luc'}) SET n.age = 40 WITH n UNWIND ['name', 'age'] AS k RETURN k, n[k]",
                    {{"age", "40"}, {"name", "Luc"}});
}

TEST_F(EntityDynamicPropertyTest, readsAPropertyOfANodeAListHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH collect(n) AS ns UNWIND ['name', 'age'] AS k RETURN k, ns[0][k]",
               {{"age", "32"}, {"name", "Remy"}});
}

TEST_F(EntityDynamicPropertyTest, filtersOnAPropertyByAKeyAVariableHolds) {
    expectRows("MATCH (n:Person) WITH n, 'age' AS k WHERE n[k] = 32 RETURN n.name", {{"Adam"}, {"Remy"}});
}
