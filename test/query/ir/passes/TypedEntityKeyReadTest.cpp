#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

bool contains(std::string_view text, std::string_view part) {
    return text.find(part) != std::string_view::npos;
}

size_t occurrencesOf(std::string_view text, std::string_view part) {
    size_t count = 0;
    size_t position = text.find(part);

    while (position != std::string_view::npos) {
        count++;
        position = text.find(part, position + part.size());
    }

    return count;
}

}

// n[k] whose key holds one string in every row reads the property the graph types under that
// name, as n.name does, and hands its values to the readers of the tagged cell the read by
// name made. A comparison with a value of the same kind compares the typed column itself.
class TypedEntityKeyReadTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        Rows rows;
        sortedRowsOf(query, rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectWriteRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runWrite(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void sortedRowsOf(std::string_view query, Rows& rows) {
        StringRowSink sink;
        runQuery(query, sink);

        sink.sortedRows(rows);
    }

    void programOf(std::string_view dialect, std::string_view query, std::string& program) {
        StringRowSink sink;
        runWrite("EXPLAIN (" + std::string(dialect) + ") " + std::string(query), sink);

        ASSERT_EQ(sink.getRows().size(), 1u);
        program = sink.getRows().front().back();
    }

    void expectTypedRead(std::string_view query, std::string_view fetch, std::string_view property) {
        std::string program;
        programOf("db", query, program);

        const std::string typedRead = std::string(fetch) + "(%";
        const std::string propertyName = "\"" + std::string(property) + "\")";
        const std::string keyRead = "key \"" + std::string(property) + "\"";

        EXPECT_TRUE(contains(program, typedRead)) << program;
        EXPECT_TRUE(contains(program, propertyName)) << program;
        EXPECT_FALSE(contains(program, keyRead)) << program;
        EXPECT_FALSE(contains(program, "db.dynamic_map_key")) << program;
        EXPECT_FALSE(contains(program, "db.container_index")) << program;
    }

    void expectPlansAsTheDottedForm(std::string_view keyQuery, std::string_view dottedQuery) {
        std::string keyProgram;
        programOf("db", keyQuery, keyProgram);
        EXPECT_FALSE(contains(keyProgram, "db.static_map_key")) << keyProgram;
        EXPECT_FALSE(contains(keyProgram, "db.to_any")) << keyProgram;

        std::string keyLowered;
        programOf("nl", keyQuery, keyLowered);

        std::string dottedLowered;
        programOf("nl", dottedQuery, dottedLowered);

        EXPECT_EQ(keyLowered, dottedLowered) << keyQuery;
    }

    // The read by name is the one a key held in an unwound column makes, per row
    void expectReadsAsByName(std::string_view keyQuery, std::string_view byNameQuery, const Rows& expected) {
        std::string byNameProgram;
        programOf("db", byNameQuery, byNameProgram);
        EXPECT_TRUE(contains(byNameProgram, "db.dynamic_map_key")) << byNameProgram;

        expectRows(keyQuery, expected);
        expectRows(byNameQuery, expected);
    }
};

TEST_F(TypedEntityKeyReadTest, readsANodePropertyTheGraphTypes) {
    const std::string_view query = "MATCH (n:Person) WITH n, 'name' AS k RETURN n[k]";

    expectTypedRead(query, "db.get_node_properties", "name");
    expectRows(query, {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}});

    const std::string_view mapQuery = "MATCH (n:Person) WITH n, {k: 'age'} AS m RETURN n.name, n[m.k]";

    expectTypedRead(mapQuery, "db.get_node_properties", "age");
    expectRows(mapQuery, {{"Adam", "32"}, {"Cyrus", "null"}, {"Doruk", "null"}, {"Luc", "null"},
                          {"Martina", "null"}, {"Maxime", "null"}, {"Remy", "32"}, {"Suhas", "null"}});
}

TEST_F(TypedEntityKeyReadTest, readsAnEdgePropertyTheGraphTypes) {
    const std::string_view query = "MATCH ()-[e]->() WITH e, 'duration' AS k RETURN e[k]";

    expectTypedRead(query, "db.get_edge_properties", "duration");
    expectRows(query, {{"10"}, {"15"}, {"20"}, {"20"}, {"20"}, {"20"}, {"20"}, {"200"},
                       {"null"}, {"null"}, {"null"}, {"null"}, {"null"},
                       {"null"}, {"null"}, {"null"}, {"null"}, {"null"}});

    const std::string_view mapQuery = "MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() "
                                      "WITH e, {k: 'name'} AS m RETURN e[m.k]";

    expectTypedRead(mapQuery, "db.get_edge_properties", "name");
    expectRows(mapQuery, {{"Remy -> Adam"}});
}

TEST_F(TypedEntityKeyReadTest, readsNullOffAnEntityAnOptionalMatchMissed) {
    const std::string_view nodeQuery = "OPTIONAL MATCH (n:Person {name: 'Nobody'}) WITH n, 'name' AS k RETURN n[k]";

    expectTypedRead(nodeQuery, "db.get_node_properties", "name");
    expectRows(nodeQuery, {{"null"}});

    const std::string_view endQuery = "MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
                                      "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f:Person) WITH p, f, 'age' AS k "
                                      "RETURN p.name, f[k]";

    expectRows(endQuery, {{"Luc", "null"}, {"Remy", "32"}});

    const std::string_view edgeQuery = "MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
                                       "OPTIONAL MATCH (p)-[e:KNOWS_WELL]->() WITH p, e, 'duration' AS k "
                                       "RETURN p.name, e[k]";

    expectTypedRead(edgeQuery, "db.get_edge_properties", "duration");
    expectRows(edgeQuery, {{"Luc", "null"}, {"Remy", "20"}});
}

TEST_F(TypedEntityKeyReadTest, readsAPropertyOfAnEntityAValueHolds) {
    const std::string_view mapQuery = "MATCH (n:Person {name: 'Remy'}) WITH {who: n} AS m, 'name' AS k RETURN m.who[k]";

    expectTypedRead(mapQuery, "db.get_node_properties", "name");
    expectRows(mapQuery, {{"Remy"}});

    const std::string_view listQuery = "MATCH (n:Person {name: 'Remy'}) WITH collect(n) AS ns, 'age' AS k "
                                       "RETURN ns[0][k], ns[5][k]";

    expectTypedRead(listQuery, "db.get_node_properties", "age");
    expectRows(listQuery, {{"32", "null"}});

    const std::string_view edgeQuery = "MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() "
                                       "WITH collect(e) AS es, 'duration' AS k RETURN es[0][k]";

    expectTypedRead(edgeQuery, "db.get_edge_properties", "duration");
    expectRows(edgeQuery, {{"20"}});
}

TEST_F(TypedEntityKeyReadTest, keepsAReadByNameOfAValueHoldingOtherValuesThanEntitiesOfOneKind) {
    const std::string_view mapQuery = "MATCH (n:Person {name: 'Remy'}) UNWIND [n, {name: 'x'}] AS v "
                                      "WITH v, 'name' AS k RETURN v[k]";

    std::string program;
    programOf("db", mapQuery, program);
    EXPECT_TRUE(contains(program, "db.static_map_key")) << program;
    EXPECT_FALSE(contains(program, "db.to_any")) << program;

    expectRows(mapQuery, {{"Remy"}, {"x"}});

    const std::string_view entityQuery = "MATCH (n:Person {name: 'Remy'})-[e:KNOWS_WELL]->() UNWIND [n, e] AS v "
                                         "WITH v, 'name' AS k RETURN v[k]";

    programOf("db", entityQuery, program);
    EXPECT_TRUE(contains(program, "db.static_map_key")) << program;
    EXPECT_FALSE(contains(program, "db.to_any")) << program;

    expectRows(entityQuery, {{"Remy"}, {"Remy -> Adam"}});
}

TEST_F(TypedEntityKeyReadTest, keepsAReadByNameOfANameTheGraphDoesNotType) {
    const std::string_view query = "MATCH (n:Person) WITH n, 'nosuch' AS k RETURN n[k]";

    std::string program;
    programOf("db", query, program);
    EXPECT_TRUE(contains(program, "db.static_map_key")) << program;
    EXPECT_FALSE(contains(program, "db.to_any")) << program;

    expectRows(query, {{"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}});
}

TEST_F(TypedEntityKeyReadTest, readsEveryValueTypeOfANodeAsTheReadByNameDoes) {
    runWrite("CREATE (:Typed {title: 'full', rank: 7, score: 2.5, flag: true, tags: [1, 'a', [2, 3]], "
             "attrs: {a: 1, b: 'y', c: [true]}, seen: datetime('2024-01-02T03:04:05Z'), "
             "span: duration({hours: 25, minutes: 1}), spot: point({x: 1.5, y: -2.0}), vec: (1.5, -2.0)}), "
             "(:Typed {title: 'bare'})");

    const std::vector<std::string_view> properties = {"title", "rank", "score", "flag", "tags", "attrs", "seen", "span", "spot", "vec"};
    const std::vector<Rows> expected = {
        {{"bare"}, {"full"}},
        {{"7"}, {"null"}},
        {{"2.5"}, {"null"}},
        {{"null"}, {"true"}},
        {{"[1, a, [2, 3]]"}, {"null"}},
        {{"null"}, {"{a: 1, b: y, c: [true]}"}},
        {{"2024-01-02T03:04:05Z"}, {"null"}},
        {{"PT25H1M"}, {"null"}},
        {{"null"}, {"point({x: 1.5, y: -2.0, crs: 'cartesian'})"}},
        {{"[1.5, -2]"}, {"null"}},
    };

    for (size_t index = 0; index < properties.size(); index++) {
        const std::string property(properties[index]);
        const std::string keyQuery = "MATCH (n:Typed) WITH n, '" + property + "' AS k RETURN n[k]";
        const std::string byNameQuery = "UNWIND ['" + property + "'] AS k MATCH (n:Typed) RETURN n[k]";

        expectTypedRead(keyQuery, "db.get_node_properties", property);
        expectReadsAsByName(keyQuery, byNameQuery, expected[index]);
    }
}

TEST_F(TypedEntityKeyReadTest, readsEveryValueTypeOfAnEdgeAsTheReadByNameDoes) {
    runWrite("CREATE (:Typed {title: 'a'})-[:LINKS {title: 'full', rank: 7, score: 2.5, flag: true, "
             "tags: [1, 'a', [2, 3]], attrs: {a: 1, b: 'y', c: [true]}, seen: datetime('2024-01-02T03:04:05Z'), "
             "span: duration({hours: 25, minutes: 1}), spot: point({x: 1.5, y: -2.0}), vec: (1.5, -2.0)}]->(:Typed {title: 'b'}), "
             "(:Typed {title: 'c'})-[:LINKS {title: 'bare'}]->(:Typed {title: 'd'})");

    const std::vector<std::string_view> properties = {"title", "rank", "score", "flag", "tags", "attrs", "seen", "span", "spot", "vec"};
    const std::vector<Rows> expected = {
        {{"bare"}, {"full"}},
        {{"7"}, {"null"}},
        {{"2.5"}, {"null"}},
        {{"null"}, {"true"}},
        {{"[1, a, [2, 3]]"}, {"null"}},
        {{"null"}, {"{a: 1, b: y, c: [true]}"}},
        {{"2024-01-02T03:04:05Z"}, {"null"}},
        {{"PT25H1M"}, {"null"}},
        {{"null"}, {"point({x: 1.5, y: -2.0, crs: 'cartesian'})"}},
        {{"[1.5, -2]"}, {"null"}},
    };

    for (size_t index = 0; index < properties.size(); index++) {
        const std::string property(properties[index]);
        const std::string keyQuery = "MATCH ()-[e:LINKS]->() WITH e, '" + property + "' AS k RETURN e[k]";
        const std::string byNameQuery = "UNWIND ['" + property + "'] AS k MATCH ()-[e:LINKS]->() RETURN e[k]";

        expectTypedRead(keyQuery, "db.get_edge_properties", property);
        expectReadsAsByName(keyQuery, byNameQuery, expected[index]);
    }
}

TEST_F(TypedEntityKeyReadTest, scansNodesByTheValueAKeyedEqualityNames) {
    const std::string_view keyQuery = "MATCH (n:Person) WITH n, {k: 'name'} AS m WHERE n[m.k] = 'Remy' RETURN n.name";
    const std::string_view dottedQuery = "MATCH (n:Person) WHERE n.name = 'Remy' RETURN n.name";

    std::string program;
    programOf("db", keyQuery, program);
    EXPECT_TRUE(contains(program, "db.scan_nodes_by_property_value")) << program;

    expectPlansAsTheDottedForm(keyQuery, dottedQuery);
    expectRows(keyQuery, {{"Remy"}});

    const std::string_view leftQuery = "MATCH (n:Person) WITH n, 'name' AS k WHERE 'Remy' = n[k] RETURN n.name";
    const std::string_view leftDottedQuery = "MATCH (n:Person) WHERE 'Remy' = n.name RETURN n.name";

    expectPlansAsTheDottedForm(leftQuery, leftDottedQuery);
    expectRows(leftQuery, {{"Remy"}});
}

TEST_F(TypedEntityKeyReadTest, comparesTheTypedColumn) {
    expectPlansAsTheDottedForm("MATCH (n:Person) WITH n, 'age' AS k WHERE n[k] > 30 RETURN n.name",
                               "MATCH (n:Person) WHERE n.age > 30 RETURN n.name");
    expectRows("MATCH (n:Person) WITH n, 'age' AS k WHERE n[k] > 30 RETURN n.name", {{"Adam"}, {"Remy"}});

    expectPlansAsTheDottedForm("MATCH (n:Person) WITH n, 'age' AS k WHERE n[k] = 32.0 RETURN n.name",
                               "MATCH (n:Person) WHERE n.age = 32.0 RETURN n.name");
    expectRows("MATCH (n:Person) WITH n, 'age' AS k WHERE n[k] = 32.0 RETURN n.name", {{"Adam"}, {"Remy"}});

    expectPlansAsTheDottedForm("MATCH ()-[e]->() WITH e, 'duration' AS k WHERE e[k] = 20 RETURN e.name",
                               "MATCH ()-[e]->() WHERE e.duration = 20 RETURN e.name");
    expectRows("MATCH ()-[e]->() WITH e, 'duration' AS k WHERE e[k] = 20 RETURN e.name",
               {{"Adam -> Remy"}, {"Luc -> Animals"}, {"Remy -> Adam"}, {"Remy -> Eighties"}, {"Remy -> Ghosts"}});

    expectPlansAsTheDottedForm("MATCH ()-[e]->() WITH e, 'duration' AS k WHERE e[k] <> 20 RETURN e.name",
                               "MATCH ()-[e]->() WHERE e.duration <> 20 RETURN e.name");
    expectRows("MATCH ()-[e]->() WITH e, 'duration' AS k WHERE e[k] <> 20 RETURN e.name",
               {{"Ghosts -> Remy"}, {"Luc -> Computers"}, {"Martina -> Cooking"}});

    const std::string_view columnsQuery = "MATCH (a:Person), (b:Person) WITH a, b, 'age' AS k WHERE a[k] = b[k] "
                                          "RETURN a.name, b.name";

    expectPlansAsTheDottedForm(columnsQuery, "MATCH (a:Person), (b:Person) WHERE a.age = b.age RETURN a.name, b.name");
    expectRows(columnsQuery, {{"Adam", "Adam"}, {"Adam", "Remy"}, {"Remy", "Adam"}, {"Remy", "Remy"}});

    expectPlansAsTheDottedForm("MATCH (a:Person), (b:Person) WITH a, b, 'name' AS k WHERE a[k] < b.name "
                               "AND b.name = 'Doruk' RETURN a.name",
                               "MATCH (a:Person), (b:Person) WHERE a.name < b.name AND b.name = 'Doruk' RETURN a.name");
}

TEST_F(TypedEntityKeyReadTest, comparesWithAConstantOfAnotherTypeAsTheReadByNameDoes) {
    expectRows("MATCH (n:Person) WITH n, 'age' AS k WHERE n[k] = 'x' RETURN n.name", {});
    expectRows("MATCH (n:Person) WITH n, 'age' AS k WHERE n[k] <> 'x' RETURN n.name", {{"Adam"}, {"Remy"}});
    expectRows("MATCH (n:Person) WITH n, 'name' AS k WHERE n[k] < 1 RETURN n.name", {});
}

TEST_F(TypedEntityKeyReadTest, sharesTheReadOfTheDottedForm) {
    const std::string_view query = "MATCH (n:Person) WITH n, 'name' AS k RETURN n.name, n[k]";

    std::string program;
    programOf("db", query, program);
    EXPECT_EQ(occurrencesOf(program, "db.get_node_properties"), 1u) << program;

    expectRows(query, {{"Adam", "Adam"}, {"Cyrus", "Cyrus"}, {"Doruk", "Doruk"}, {"Luc", "Luc"},
                       {"Martina", "Martina"}, {"Maxime", "Maxime"}, {"Remy", "Remy"}, {"Suhas", "Suhas"}});
}

TEST_F(TypedEntityKeyReadTest, readsWhatTheQueryWrote) {
    const std::string_view createQuery = "CREATE (n:Thing {name: 'Apa'}) WITH n, 'name' AS k RETURN n[k]";

    expectTypedRead(createQuery, "db.get_node_properties", "name");
    expectWriteRows(createQuery, {{"Apa"}});

    expectWriteRows("MATCH (n:Person {name: 'Luc'}) SET n.age = 40 WITH n, 'age' AS k RETURN n[k]", {{"40"}});
    expectWriteRows("MERGE (n:Person {name: 'Zoe'}) WITH n, 'name' AS k RETURN n[k]", {{"Zoe"}});
    expectWriteRows("MERGE (n:Person {name: 'Remy'}) WITH n, 'age' AS k RETURN n[k]", {{"32"}});

    expectWriteRows("MATCH (a:Person {name: 'Luc'}), (b:Person {name: 'Remy'}) "
                    "CREATE (a)-[e:KNOWS_WELL {duration: 5}]->(b) WITH e, 'duration' AS k RETURN e[k]",
                    {{"5"}});
    expectWriteRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() SET e.duration = 7 "
                    "WITH e, 'duration' AS k RETURN e[k]",
                    {{"7"}});
}

TEST_F(TypedEntityKeyReadTest, readsANameOnlyTheQueryCreates) {
    expectWriteRows("CREATE (n:Thing {fresh: 1}) WITH n, 'fresh' AS k RETURN n[k]", {{"1"}});
    expectWriteRows("CREATE (:Thing)-[e:LINKS {fresh: 2}]->(:Thing) WITH e, 'fresh' AS k RETURN e[k]", {{"2"}});
}

TEST_F(TypedEntityKeyReadTest, keepsAKeyThatIsNotConstantAReadPerRow) {
    const std::string_view nodeQuery = "UNWIND ['name', 'age'] AS k MATCH (n:Person {name: 'Remy'}) RETURN k, n[k]";

    std::string program;
    programOf("db", nodeQuery, program);
    EXPECT_TRUE(contains(program, "db.dynamic_map_key")) << program;
    EXPECT_FALSE(contains(program, "db.to_any")) << program;

    expectRows(nodeQuery, {{"age", "32"}, {"name", "Remy"}});

    const std::string_view edgeQuery = "UNWIND ['name', 'duration'] AS k "
                                       "MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() RETURN k, e[k]";

    programOf("db", edgeQuery, program);
    EXPECT_TRUE(contains(program, "db.dynamic_map_key")) << program;
    EXPECT_FALSE(contains(program, "db.to_any")) << program;

    expectRows(edgeQuery, {{"duration", "20"}, {"name", "Remy -> Adam"}});
}
