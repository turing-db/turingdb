#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

bool contains(std::string_view text, std::string_view part) {
    return text.find(part) != std::string_view::npos;
}

}

// simpledb's Person nodes in id order are Remy, Adam, Maxime, Luc, Martina, Suhas, Cyrus and
// Doruk, so a collect of them starts with Remy; Remy and Adam are the two aged 32. Its
// KNOWS_WELL edges are Remy -> Adam and Adam -> Remy, both of duration 20, and Ghosts -> Remy
// of duration 200.
class EmptyListUnificationTest : public CallV3Test {
protected:
    void expectSortedRows(const std::string& query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
        std::sort(expected.begin(), expected.end());

        EXPECT_EQ(rows, expected) << query;
    }

    // An element of a list that keeps its type is read as the entity it is: the property
    // read goes through the node or the edge, never through a map key of a tagged cell
    void expectReadsThroughTheEntity(const std::string& query, std::string_view propertyRead) {
        StringRowSink sink;
        runQuery("EXPLAIN (nl) " + query, sink);

        const Rows& rows = sink.getRows();
        ASSERT_EQ(rows.size(), 1u);

        const std::string& program = rows.front().back();

        EXPECT_TRUE(contains(program, propertyRead)) << query << "\n" << program;
        EXPECT_FALSE(contains(program, "map_element")) << query << "\n" << program;
    }

    void storeTags() {
        runWrite("MATCH (n:Person {name: 'Remy'}) SET n.tags = ['x', 'y']");
        runWrite("MATCH (n:Person {name: 'Adam'}) SET n.tags = ['z']");
    }
};

TEST_F(EmptyListUnificationTest, coalescesACollectedListWithAnEmptyOne) {
    const std::string query = "MATCH (a:Person) WITH collect(a) AS l RETURN coalesce(l, [])[0].name, coalesce([], l)[0].name";

    expectSortedRows(query, {{"Remy", "null"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, choosesBetweenACollectedListAndAnEmptyOne) {
    const std::string query = "MATCH (a:Person) WITH collect(a) AS l "
                              "RETURN CASE WHEN true THEN l ELSE [] END[0].name, "
                              "CASE WHEN false THEN l ELSE [] END[0].name, "
                              "CASE WHEN true THEN [] ELSE l END[0].name";

    expectSortedRows(query, {{"Remy", "null", "null"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, concatenatesACollectedListWithAnEmptyOne) {
    const std::string query = "MATCH (a:Person) WITH collect(a) AS l RETURN (l + [])[0].name, ([] + l)[0].name, (l || [])[0].name";

    expectSortedRows(query, {{"Remy", "Remy", "Remy"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, appendsANodeToAnEmptyList) {
    const std::string query = "MATCH (a:Person) RETURN ([] + a)[0].name, (a + [])[0].name";

    expectSortedRows(query,
                     {{"Remy", "Remy"},
                      {"Adam", "Adam"},
                      {"Maxime", "Maxime"},
                      {"Luc", "Luc"},
                      {"Martina", "Martina"},
                      {"Suhas", "Suhas"},
                      {"Cyrus", "Cyrus"},
                      {"Doruk", "Doruk"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, nestsACollectedListBesideAnEmptyOne) {
    const std::string query = "MATCH (a:Person) WITH collect(a) AS l RETURN [[], l][1][0].name, [l, []][0][0].name";

    expectSortedRows(query, {{"Remy", "Remy"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, readsANodeOutOfANestedCollectedList) {
    const std::string query = "MATCH (a:Person) WITH collect(a) AS l RETURN [l, l][1][0].name, [l][0][-1].name";

    expectSortedRows(query, {{"Remy", "Doruk"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, joinsAListOfNullsWithACollectedList) {
    const std::string query = "MATCH (a:Person) WITH collect(a) AS l "
                              "RETURN coalesce([null], l)[0].name, CASE WHEN true THEN [null] ELSE l END[0].name, "
                              "([null] + l)[1].name";

    expectSortedRows(query, {{"null", "null", "Remy"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, reducesIntoAnEmptyList) {
    const std::string query = "MATCH (a:Person) WITH collect(a) AS l "
                              "RETURN reduce(acc = [], x IN l | acc + x)[0].name, reduce(acc = [], x IN l | [x] + acc)[-1].name";

    expectSortedRows(query, {{"Remy", "Remy"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, filtersTheNodesOfACoalescedList) {
    const std::string query = "MATCH (a:Person) WITH collect(a) AS l RETURN size([x IN coalesce(l, []) WHERE x.age = 32])";

    expectSortedRows(query, {{"2"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, coalescesAGroupWithAnEmptyList) {
    const std::string query = "MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) "
                              "RETURN n.name, coalesce(x, [])[0].name, CASE WHEN true THEN y ELSE [] END[-1].name, "
                              "([] + y)[-1].name, (y + [])[-1].name";

    expectSortedRows(query,
                     {{"Remy", "Remy", "Adam", "Adam", "Adam"},
                      {"Remy", "Remy", "Remy", "Remy", "Remy"},
                      {"Adam", "Adam", "Remy", "Remy", "Remy"},
                      {"Adam", "Adam", "Adam", "Adam", "Adam"},
                      {"Ghosts", "Ghosts", "Remy", "Remy", "Remy"},
                      {"Ghosts", "Ghosts", "Adam", "Adam", "Adam"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, coalescesTheRelationshipsOfAWalkWithAnEmptyList) {
    const std::string query = "MATCH (n)-[r:KNOWS_WELL*1..2]->(c) "
                              "RETURN n.name, [e IN coalesce(r, []) | e.duration], coalesce(r, [])[0].name, "
                              "CASE WHEN true THEN r ELSE [] END[-1].name, ([] + r)[0].duration";

    expectSortedRows(query,
                     {{"Remy", "20", "Remy -> Adam", "Remy -> Adam", "20"},
                      {"Remy", "20, 20", "Remy -> Adam", "Adam -> Remy", "20"},
                      {"Adam", "20", "Adam -> Remy", "Adam -> Remy", "20"},
                      {"Adam", "20, 20", "Adam -> Remy", "Remy -> Adam", "20"},
                      {"Ghosts", "200", "Ghosts -> Remy", "Ghosts -> Remy", "200"},
                      {"Ghosts", "200, 20", "Ghosts -> Remy", "Remy -> Adam", "200"}});
    expectReadsThroughTheEntity(query, "nl.get_edge_properties");
}

TEST_F(EmptyListUnificationTest, publishesAUnionOfACollectedListAndAnEmptyOne) {
    const std::string query = "CALL { MATCH (a:Person) RETURN collect(a) AS l UNION RETURN [] AS l } RETURN l[0].name";

    expectSortedRows(query, {{"Remy"}, {"null"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, publishesAUnionOfAnEmptyListAndACollectedOne) {
    const std::string query = "CALL { RETURN [] AS l UNION MATCH (a:Person) RETURN collect(a) AS l } RETURN l[0].name";

    expectSortedRows(query, {{"Remy"}, {"null"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, collectsAUnionOfACollectedListAndAnEmptyOne) {
    const std::string query = "RETURN [l IN COLLECT { MATCH (a:Person) RETURN collect(a) AS l UNION RETURN [] AS l } "
                              "WHERE size(l) > 0][0][0].name";

    expectSortedRows(query, {{"Remy"}});
    expectReadsThroughTheEntity(query, "nl.get_node_properties");
}

TEST_F(EmptyListUnificationTest, unitesListsOfDifferentElementTypes) {
    expectSortedRows("RETURN [1] AS l UNION RETURN ['a'] AS l", {{"1"}, {"a"}});
    expectSortedRows("RETURN [] AS l UNION RETURN [1] AS l", {{""}, {"1"}});
    expectSortedRows("RETURN [[1]] AS l UNION RETURN [['a'], []] AS l", {{"[1]"}, {"[a], []"}});
}

TEST_F(EmptyListUnificationTest, unitesListsOfDifferentElementTypesInACallBody) {
    expectSortedRows("CALL { RETURN [1] AS l UNION RETURN ['a'] AS l } RETURN l", {{"1"}, {"a"}});
    expectSortedRows("CALL { RETURN [] AS l UNION RETURN [1] AS l } RETURN l", {{""}, {"1"}});
}

TEST_F(EmptyListUnificationTest, sumsTheElementsOfAListOfNone) {
    expectSortedRows("UNWIND [] AS v RETURN sum(v)", {{"0"}});
    expectSortedRows("UNWIND [null, null] AS v RETURN sum(v)", {{"0"}});
}

TEST_F(EmptyListUnificationTest, readsAStoredList) {
    storeTags();

    expectSortedRows("MATCH (n:Person) WHERE n.tags IS NOT NULL "
                     "RETURN n.name, n.tags, n.tags[0], n.tags[-1], size(n.tags)",
                     {{"Remy", "x, y", "x", "y", "2"},
                      {"Adam", "z", "z", "z", "1"}});

    expectSortedRows("MATCH (n:Person {name: 'Remy'}) UNWIND n.tags AS t RETURN t", {{"x"}, {"y"}});
}

TEST_F(EmptyListUnificationTest, comparesAStoredList) {
    storeTags();

    expectSortedRows("MATCH (n:Person) WHERE n.tags IS NOT NULL "
                     "RETURN n.name, n.tags = ['x', 'y'], n.tags = [], 'x' IN n.tags",
                     {{"Remy", "true", "false", "true"},
                      {"Adam", "false", "false", "false"}});
}

TEST_F(EmptyListUnificationTest, coalescesAStoredListWithATypedList) {
    storeTags();

    expectSortedRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Adam', 'Luc'] "
                     "RETURN n.name, coalesce(n.tags, ['none']), coalesce(n.tags, []), "
                     "CASE WHEN n.tags IS NULL THEN [] ELSE n.tags END, n.tags + ['w'], n.tags + []",
                     {{"Remy", "x, y", "x, y", "x, y", "x, y, w", "x, y"},
                      {"Adam", "z", "z", "z", "z, w", "z"},
                      {"Luc", "none", "", "", "null", "null"}});

    expectSortedRows("MATCH (a:Person) WITH collect(a) AS l MATCH (n:Person {name: 'Remy'}) "
                     "RETURN coalesce(n.tags, l)[0], coalesce(n.tags, l)[1]",
                     {{"x", "y"}});
}

TEST_F(EmptyListUnificationTest, unitesAStoredListWithATypedList) {
    storeTags();

    expectSortedRows("MATCH (n:Person {name: 'Remy'}) RETURN n.tags AS l UNION RETURN ['w'] AS l", {{"x, y"}, {"w"}});
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) RETURN n.tags AS l UNION RETURN ['x', 'y'] AS l", {{"x, y"}});
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) RETURN n.tags AS l UNION RETURN [] AS l", {{"x, y"}, {""}});
    expectSortedRows("CALL { MATCH (n:Person {name: 'Remy'}) RETURN n.tags AS l UNION RETURN [1] AS l } RETURN l[0]",
                     {{"x"}, {"1"}});
}

TEST_F(EmptyListUnificationTest, readsAProcedureList) {
    expectSortedRows("CALL db.getNodes([0]) YIELD labels "
                     "RETURN size(labels), 'Person' IN labels, labels = labels, coalesce(labels, ['z']) = labels, "
                     "size(coalesce(labels, [])), size(labels + []), size(labels + ['z'])",
                     {{"3", "true", "true", "true", "3", "3", "4"}});
}

TEST_F(EmptyListUnificationTest, unitesAProcedureListWithATypedList) {
    expectSortedRows("CALL { CALL db.getNodes([0]) YIELD labels RETURN labels AS l UNION RETURN ['z'] AS l } "
                     "RETURN size(l)",
                     {{"3"}, {"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
