#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <array>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

#include "CellView.h"
#include "ID.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "map/MapBuffer.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"
#include "metadata/DateTime.h"
#include "metadata/Duration.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;
using MapEntries = std::vector<MapBuffer<>::MapKeyValuePair>;
using ListElements = std::vector<QueryListBuffer::ListItemVariant>;

void expectCell(const Column& column, std::string_view expected) {
    const std::array<const Column*, 1> chunks {&column};

    StringRowSink sink;
    sink.appendChunks(chunks, 0, 1);

    EXPECT_EQ(sink.getRows().front().front(), expected);
}

}

// simpledb's Person nodes are Remy, Adam, Maxime, Luc, Martina, Suhas, Cyrus and Doruk, in
// id order. Remy is 32, born 18/01, French and a PhD; his one KNOWS_WELL edge goes to Adam,
// is named "Remy -> Adam" and lasts 20. Luc has no KNOWS_WELL edge.
class StringRowSinkTest : public CallV3Test {
protected:
    void expectSortedRows(const std::string& query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
        std::sort(expected.begin(), expected.end());

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(StringRowSinkTest, readsAMapValuePerRow) {
    expectSortedRows("MATCH (n:Person) WITH n, {name: n.name} AS m RETURN m.name",
                     {{"Remy"},
                      {"Adam"},
                      {"Maxime"},
                      {"Luc"},
                      {"Martina"},
                      {"Suhas"},
                      {"Cyrus"},
                      {"Doruk"}});
}

TEST_F(StringRowSinkTest, readsTheValuesOfAConstantMap) {
    expectSortedRows("WITH {a: 1, b: 'x', c: [1, 2], d: {e: true}} AS m RETURN m.a, m.b, m.c, m.d, m.z",
                     {{"1", "x", "[1, 2]", "{e: true}", "null"}});
}

TEST_F(StringRowSinkTest, readsANameOutOfAnElementOfAMixedList) {
    expectSortedRows("MATCH (a:Person) WITH collect(a) AS l RETURN coalesce(l, [1])[0].name",
                     {{"Remy"}});
}

TEST_F(StringRowSinkTest, readsTheMapOfANodeAndOfAnEdge) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'})-[e:KNOWS_WELL]->() RETURN properties(n), properties(e)",
                     {{"{age: 32, dob: 18/01, hasPhD: true, isFrench: true, name: Remy}",
                       "{duration: 20, name: Remy -> Adam}"}});
}

TEST_F(StringRowSinkTest, readsAnAbsentMap) {
    expectSortedRows("MATCH (n:Person {name: 'Luc'}) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m) RETURN properties(m)",
                     {{"null"}});
}

TEST_F(StringRowSinkTest, readsAConstantMapAndAListOfMaps) {
    expectSortedRows("RETURN {b: 2, a: 'x'}, [{a: 1}, {a: [2, 3]}]",
                     {{"{a: x, b: 2}", "{a: 1}, {a: [2, 3]}"}});
}

TEST(StringRowSinkColumnTest, readsAMapValueOfEachTag) {
    MapBuffer<> maps;
    QueryListBuffer lists;

    const float embedding[] {0.5f, 1.5f};
    const ListElements listElements {int64_t {1}, std::string_view {"a"}};
    const MapEntries nestedEntries {{.key = "k", .value = int64_t {7}}};

    const MapEntries entries {
        {.key = "a", .value = int64_t {-1}},
        {.key = "b", .value = uint64_t {2}},
        {.key = "c", .value = 2.5},
        {.key = "d", .value = CustomBool {true}},
        {.key = "e", .value = std::string_view {"text"}},
        {.key = "f", .value = types::Embedding::Primitive {embedding}},
        {.key = "g", .value = lists.insert(listElements)},
        {.key = "h", .value = maps.insert(nestedEntries)},
        {.key = "i", .value = PropertyNull {}},
        {.key = "j", .value = NodeID {3}},
        {.key = "k", .value = EdgeID {4}},
        {.key = "l", .value = DateTime {0}},
        {.key = "m", .value = Duration {90061000000}},
    };

    const MapView map = maps.insert(entries);

    ColumnVector<CellView> values;
    for (const MapEntryView entry : map) {
        values.push_back(entry.getValue());
    }

    const std::vector<std::string> expected {
        "-1", "2", "2.5", "true", "text", "[0.5, 1.5]", "[1, a]", "{k: 7}", "null", "3", "4",
        "1970-01-01T00:00:00Z", "PT25H1M1S",
    };

    const std::array<const Column*, 1> chunks {&values};

    StringRowSink sink;
    sink.appendChunks(chunks, 0, values.size());

    std::vector<std::string> rendered;
    for (const StringRowSink::Row& row : sink.getRows()) {
        rendered.push_back(row.front());
    }

    EXPECT_EQ(rendered, expected);

    expectCell(ColumnConst<CellView> {map.front().getValue()}, "-1");
}

TEST(StringRowSinkColumnTest, readsAMap) {
    MapBuffer<> maps;
    const MapEntries entries {{.key = "a", .value = int64_t {1}}, {.key = "b", .value = PropertyNull {}}};
    const MapView map = maps.insert(entries);

    expectCell(ColumnVector<MapView> {map}, "{a: 1, b: null}");
    expectCell(ColumnConst<MapView> {MapView {map}}, "{a: 1, b: null}");
    expectCell(ColumnVector<std::optional<MapView>> {std::optional<MapView> {}}, "null");
    expectCell(ColumnConst<std::optional<MapView>> {std::optional<MapView> {map}}, "{a: 1, b: null}");
}

TEST(StringRowSinkColumnTest, readsAListHoldingAnEmbeddingAndAMap) {
    MapBuffer<> maps;
    QueryListBuffer lists;

    const float embedding[] {0.5f, 1.5f};
    const MapEntries entries {{.key = "a", .value = int64_t {1}}};
    const ListElements elements {types::Embedding::Primitive {embedding}, maps.insert(entries)};
    const ListView list = lists.insert(elements);

    expectCell(ColumnVector<ListView> {list}, "[0.5, 1.5], {a: 1}");
    expectCell(ColumnVector<CellView> {list.front()}, "[0.5, 1.5]");
}

TEST(StringRowSinkColumnTest, readsAnEmbedding) {
    const float embedding[] {0.5f, 1.5f};
    const types::Embedding::Primitive values {embedding};

    expectCell(ColumnVector<types::Embedding::Primitive> {values}, "0.5, 1.5");
    expectCell(ColumnConst<types::Embedding::Primitive> {types::Embedding::Primitive {values}}, "0.5, 1.5");
    expectCell(ColumnVector<std::optional<types::Embedding::Primitive>> {std::nullopt}, "null");
}

TEST(StringRowSinkColumnTest, readsIdentifiersAndHashes) {
    expectCell(ColumnConst<NodeID> {NodeID {5}}, "5");
    expectCell(ColumnVector<LabelSetID> {LabelSetID {2}}, "2");
    expectCell(ColumnVector<std::optional<EdgeTypeID>> {EdgeTypeID {1}}, "1");
    expectCell(ColumnVector<std::optional<LabelID>> {std::nullopt}, "null");
    expectCell(ColumnVector<std::optional<PropertyTypeID>> {PropertyTypeID {3}}, "3");
    expectCell(ColumnVector<CommitHash> {CommitHash {255}}, "ff");
    expectCell(ColumnVector<ChangeID> {ChangeID {255}}, "255");
}

TEST(StringRowSinkColumnTest, readsBooleansTypesAndConstantTemporals) {
    expectCell(ColumnVector<CustomBool> {CustomBool {true}}, "true");
    expectCell(ColumnConst<CustomBool> {CustomBool {false}}, "false");
    expectCell(ColumnConst<ValueType> {ValueType::String}, "String");
    expectCell(ColumnVector<std::optional<ValueType>> {std::nullopt}, "null");
    expectCell(ColumnConst<DateTime> {DateTime {0}}, "1970-01-01T00:00:00Z");
    expectCell(ColumnConst<Duration> {Duration {90061000000}}, "PT25H1M1S");
    expectCell(ColumnConst<std::optional<std::string>> {std::optional<std::string> {"text"}}, "text");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
