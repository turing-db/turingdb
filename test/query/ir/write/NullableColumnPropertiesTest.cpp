#include <gtest/gtest.h>

#include <optional>
#include <variant>
#include <vector>

#include "NLWriteProperties.h"

#include "LocalMemory.h"
#include "list/EncodedList.h"
#include "list/ListBuffer.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "map/MapBuffer.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitWriteBuffer.h"

#include "TuringTest.h"

using namespace db;

namespace {

using Embedding = types::Embedding::Primitive;
using OwnedEmbedding = std::optional<types::Embedding::OwningPrimitive>;
using String = types::String::Primitive;
using OwnedString = std::optional<types::String::OwningPrimitive>;
using OwnedList = std::optional<types::List::OwningPrimitive>;

const OwnedEmbedding& stagedEmbedding(const CommitWriteBuffer::UntypedProperty& property) {
    return std::get<OwnedEmbedding>(property.value);
}

const OwnedString& stagedString(const CommitWriteBuffer::UntypedProperty& property) {
    return std::get<OwnedString>(property.value);
}

const OwnedList& stagedList(const CommitWriteBuffer::UntypedProperty& property) {
    return std::get<OwnedList>(property.value);
}

}

TEST(NullableColumnPropertiesTest, stagesANullableEmbeddingColumnPerRow) {
    LocalMemory memory;
    const std::vector<float> values {1.0f, 2.0f};

    ColumnOptVector<Embedding>* column = memory.alloc<ColumnOptVector<Embedding>>();
    column->push_back(Embedding {values});
    column->pushNull();

    CommitWriteBuffer::UntypedProperties properties;
    extractColumnProperties(column, 2, PropertyTypeID {3}, properties);

    ASSERT_EQ(properties.size(), 2);
    EXPECT_EQ(properties[0].propertyID, PropertyTypeID {3});
    EXPECT_EQ(stagedEmbedding(properties[0]), OwnedEmbedding {values});
    EXPECT_EQ(stagedEmbedding(properties[1]), std::nullopt);
}

TEST(NullableColumnPropertiesTest, stagesANullableEmbeddingConstantOnEveryRow) {
    LocalMemory memory;
    const std::vector<float> values {1.0f, 2.0f};

    ColumnConst<std::optional<Embedding>>* present = memory.alloc<ColumnConst<std::optional<Embedding>>>();
    present->set(Embedding {values});

    CommitWriteBuffer::UntypedProperties properties;
    extractColumnProperties(present, 2, PropertyTypeID {3}, properties);

    ASSERT_EQ(properties.size(), 2);
    EXPECT_EQ(stagedEmbedding(properties[0]), OwnedEmbedding {values});
    EXPECT_EQ(stagedEmbedding(properties[1]), OwnedEmbedding {values});

    ColumnConst<std::optional<Embedding>>* absent = memory.alloc<ColumnConst<std::optional<Embedding>>>();
    absent->setOptional(std::nullopt);

    extractColumnProperties(absent, 2, PropertyTypeID {3}, properties);

    ASSERT_EQ(properties.size(), 2);
    EXPECT_EQ(stagedEmbedding(properties[0]), std::nullopt);
    EXPECT_EQ(stagedEmbedding(properties[1]), std::nullopt);
}

TEST(NullableColumnPropertiesTest, stagesANullableStringConstantOnEveryRow) {
    LocalMemory memory;

    ColumnConst<std::optional<String>>* present = memory.alloc<ColumnConst<std::optional<String>>>();
    present->set(String {"Remy"});

    CommitWriteBuffer::UntypedProperties properties;
    extractColumnProperties(present, 2, PropertyTypeID {4}, properties);

    ASSERT_EQ(properties.size(), 2);
    for (const CommitWriteBuffer::UntypedProperty& property : properties) {
        EXPECT_EQ(property.propertyID, PropertyTypeID {4});
        EXPECT_EQ(stagedString(property), OwnedString {"Remy"});
    }

    ColumnConst<std::optional<String>>* absent = memory.alloc<ColumnConst<std::optional<String>>>();
    absent->setOptional(std::nullopt);

    extractColumnProperties(absent, 2, PropertyTypeID {4}, properties);

    ASSERT_EQ(properties.size(), 2);
    for (const CommitWriteBuffer::UntypedProperty& property : properties) {
        EXPECT_EQ(stagedString(property), std::nullopt);
    }
}

TEST(NullableColumnPropertiesTest, stagesANullableListConstantOnEveryRow) {
    LocalMemory memory;
    ListBuffer<> lists;

    const std::vector<ListBuffer<>::ListItemVariant> elements {int64_t {1}, int64_t {2}};
    const ListView list = lists.insert(elements);

    ColumnConst<std::optional<ListView>>* present = memory.alloc<ColumnConst<std::optional<ListView>>>();
    present->set(list);

    CommitWriteBuffer::UntypedProperties properties;
    extractColumnProperties(present, 2, PropertyTypeID {5}, properties);

    ASSERT_EQ(properties.size(), 2);
    for (const CommitWriteBuffer::UntypedProperty& property : properties) {
        EXPECT_EQ(property.propertyID, PropertyTypeID {5});
        EXPECT_EQ(stagedList(property), OwnedList {EncodedList {list}});
    }

    ColumnConst<std::optional<ListView>>* absent = memory.alloc<ColumnConst<std::optional<ListView>>>();
    absent->setOptional(std::nullopt);

    extractColumnProperties(absent, 2, PropertyTypeID {5}, properties);

    ASSERT_EQ(properties.size(), 2);
    for (const CommitWriteBuffer::UntypedProperty& property : properties) {
        EXPECT_EQ(stagedList(property), std::nullopt);
    }
}

TEST(NullableColumnPropertiesTest, readsANullableMapConstantOnEveryRow) {
    LocalMemory memory;
    MapBuffer<> maps;

    const std::vector<MapBuffer<>::MapKeyValuePair> entries = {{"a", types::Int64::Primitive {1}}};

    ColumnConst<std::optional<MapView>>* present = memory.alloc<ColumnConst<std::optional<MapView>>>();
    present->set(maps.insert(entries));

    std::vector<WrittenMap> written;
    extractWrittenMaps(present, 3, written);

    ASSERT_EQ(written.size(), 3);
    for (const WrittenMap& map : written) {
        ASSERT_TRUE(map._map.has_value());
        EXPECT_EQ(map._map->size(), 1);
        EXPECT_EQ(map._tag, CellTag::MapView);
    }

    ColumnConst<std::optional<MapView>>* absent = memory.alloc<ColumnConst<std::optional<MapView>>>();
    absent->setOptional(std::nullopt);

    extractWrittenMaps(absent, 2, written);

    ASSERT_EQ(written.size(), 2);
    for (const WrittenMap& map : written) {
        EXPECT_FALSE(map._map.has_value());
        EXPECT_EQ(map._tag, CellTag::Null);
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
