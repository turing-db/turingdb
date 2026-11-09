#include <gtest/gtest.h>

#include <compare>
#include <optional>
#include <span>
#include <vector>

#include "CellOrder.h"
#include "CellTag.h"
#include "CellView.h"
#include "ID.h"
#include "list/EncodedList.h"
#include "list/ListBuffer.h"
#include "list/ListContainer.h"
#include "list/ListHash.h"
#include "list/ListView.h"
#include "map/EncodedMap.h"
#include "map/MapBuffer.h"
#include "map/MapContainer.h"
#include "map/MapHash.h"
#include "map/MapView.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

using namespace db;

namespace {

// A list element or a map value holding the ID of a label, an edge type or a property type,
// or a value type: a fixed-width value stored after its tag, as a node ID is
class SchemaIDCellTest : public testing::Test {
protected:
    ListView list(std::vector<ListBuffer<>::ListItemVariant> elements) {
        return _buffer.insert(elements);
    }

    MapView map(std::vector<MapBuffer<>::MapKeyValuePair> entries) {
        return _maps.insert(entries);
    }

    ListView schemaIDs() {
        return list({LabelID(3), EdgeTypeID(1), PropertyTypeID(7), ValueType::Bool});
    }

    ListBuffer<> _buffer;
    MapBuffer<> _maps;
};

}

TEST_F(SchemaIDCellTest, readsTheIDAnElementHolds) {
    const std::span<const CellView> elements = schemaIDs().elements();
    ASSERT_EQ(elements.size(), 4u);

    ASSERT_EQ(elements[0].getTag(), CellTag::LabelID);
    EXPECT_EQ(elements[0].getAs<LabelID>(), LabelID(3));

    ASSERT_EQ(elements[1].getTag(), CellTag::EdgeTypeID);
    EXPECT_EQ(elements[1].getAs<EdgeTypeID>(), EdgeTypeID(1));

    ASSERT_EQ(elements[2].getTag(), CellTag::PropertyTypeID);
    EXPECT_EQ(elements[2].getAs<PropertyTypeID>(), PropertyTypeID(7));

    ASSERT_EQ(elements[3].getTag(), CellTag::ValueType);
    EXPECT_EQ(elements[3].getAs<ValueType>(), ValueType::Bool);
}

TEST_F(SchemaIDCellTest, aContainerCopyKeepsTheIDs) {
    const ListView source = schemaIDs();

    ListContainer container;
    container.alloc(source);

    EXPECT_TRUE(container.getView(0) == source);
}

TEST_F(SchemaIDCellTest, anEncodedListDecodesItsIDs) {
    const ListView nested = list({ValueType::String, PropertyNull {}, LabelID(0)});
    const ListView source = list({LabelID(3), EdgeTypeID(1), PropertyTypeID(7), ValueType::Bool, nested});

    const EncodedList encoded(source);

    ListContainer container;
    const ListView decoded = encoded.decodeInto(container);

    ASSERT_EQ(decoded.size(), 5u);
    EXPECT_EQ(decoded.elements()[0].getAs<LabelID>(), LabelID(3));
    EXPECT_EQ(decoded.elements()[1].getAs<EdgeTypeID>(), EdgeTypeID(1));
    EXPECT_EQ(decoded.elements()[2].getAs<PropertyTypeID>(), PropertyTypeID(7));
    EXPECT_EQ(decoded.elements()[3].getAs<ValueType>(), ValueType::Bool);

    const ListView decodedNested = decoded.back().getAs<ListView>();
    EXPECT_EQ(decodedNested.front().getAs<ValueType>(), ValueType::String);
    EXPECT_EQ(decodedNested.back().getAs<LabelID>(), LabelID(0));

    EXPECT_TRUE(decoded == source);
    EXPECT_EQ(hashList(decoded), hashList(source));
    EXPECT_TRUE(EncodedList(decoded) == encoded);
}

TEST_F(SchemaIDCellTest, anEncodedMapDecodesItsIDs) {
    const MapView source = map({{.key = "edgeType", .value = EdgeTypeID(1)},
                                {.key = "label", .value = LabelID(3)},
                                {.key = "property", .value = PropertyTypeID(7)},
                                {.key = "type", .value = ValueType::Bool}});

    const EncodedMap encoded(source);

    MapContainer container;
    const MapView decoded = encoded.decodeInto(container);

    ASSERT_EQ(decoded.size(), 4u);
    EXPECT_EQ(decoded.entries()[0].getValue().getAs<EdgeTypeID>(), EdgeTypeID(1));
    EXPECT_EQ(decoded.entries()[1].getValue().getAs<LabelID>(), LabelID(3));
    EXPECT_EQ(decoded.entries()[2].getValue().getAs<PropertyTypeID>(), PropertyTypeID(7));
    EXPECT_EQ(decoded.entries()[3].getValue().getAs<ValueType>(), ValueType::Bool);

    EXPECT_TRUE(decoded == source);
    EXPECT_EQ(hashMap(decoded), hashMap(source));
}

// An ID equals an ID of its own kind holding the same number, and nothing else: not an ID of
// another kind, nor the integer it is
TEST_F(SchemaIDCellTest, equalsAnIDOfItsKindAlone) {
    const std::span<const CellView> elements = list({LabelID(3), EdgeTypeID(3), PropertyTypeID(3), ValueType::Bool, int64_t {3}}).elements();
    const CellView label = elements[0];
    const CellView edgeType = elements[1];
    const CellView propertyType = elements[2];
    const CellView valueType = elements[3];
    const CellView integer = elements[4];

    EXPECT_TRUE(label == LabelID(3));
    EXPECT_FALSE(label == LabelID(4));
    EXPECT_FALSE(edgeType == LabelID(3));
    EXPECT_FALSE(integer == LabelID(3));

    EXPECT_TRUE(edgeType == EdgeTypeID(3));
    EXPECT_FALSE(label == EdgeTypeID(3));

    EXPECT_TRUE(propertyType == PropertyTypeID(3));
    EXPECT_FALSE(propertyType == PropertyTypeID(4));
    EXPECT_FALSE(label == PropertyTypeID(3));

    EXPECT_TRUE(valueType == ValueType::Bool);
    EXPECT_FALSE(valueType == ValueType::String);
    EXPECT_FALSE(integer == ValueType::Bool);

    EXPECT_FALSE(label == edgeType);
    EXPECT_FALSE(label == integer);
    EXPECT_FALSE(propertyType == integer);
}

TEST_F(SchemaIDCellTest, idsHeldInTwoBuffersAreEqualAndHashAlike) {
    const ListView lhs = schemaIDs();
    const ListView rhs = schemaIDs();

    for (size_t index = 0; index < lhs.size(); index++) {
        EXPECT_TRUE(lhs.elements()[index] == rhs.elements()[index]);
        EXPECT_EQ(hashCell(lhs.elements()[index]), hashCell(rhs.elements()[index]));
        EXPECT_EQ(equalOrUnknown(lhs.elements()[index], rhs.elements()[index]), std::optional<bool> {true});
    }
}

// ORDER BY sorts an ID by its number and a value type by its ordinal, so a list holding them
// sorts the same way. The kinds sort after every number and before null.
TEST_F(SchemaIDCellTest, sortsAnIDAsItsColumnSorts) {
    const std::span<const CellView> elements = list({LabelID(2),
                                                     LabelID(5),
                                                     EdgeTypeID(0),
                                                     PropertyTypeID(1),
                                                     ValueType::Int64,
                                                     ValueType::Bool,
                                                     int64_t {100},
                                                     PropertyNull {}}).elements();
    const CellView labelTwo = elements[0];
    const CellView labelFive = elements[1];
    const CellView edgeType = elements[2];
    const CellView propertyType = elements[3];
    const CellView int64Type = elements[4];
    const CellView boolType = elements[5];
    const CellView number = elements[6];
    const CellView null = elements[7];

    EXPECT_EQ(labelTwo <=> labelFive, std::strong_ordering::less);
    EXPECT_EQ(int64Type <=> boolType, std::strong_ordering::less);

    EXPECT_EQ(number <=> labelTwo, std::strong_ordering::less);
    EXPECT_EQ(labelFive <=> edgeType, std::strong_ordering::less);
    EXPECT_EQ(edgeType <=> propertyType, std::strong_ordering::less);
    EXPECT_EQ(propertyType <=> int64Type, std::strong_ordering::less);
    EXPECT_EQ(boolType <=> null, std::strong_ordering::less);

    EXPECT_EQ(list({LabelID(2)}) <=> list({LabelID(5)}), std::strong_ordering::less);
}

// < and its siblings take no schema ID, so two of them, or two lists holding them, compare to null
TEST_F(SchemaIDCellTest, comparesNoIDWithLessThan) {
    const std::span<const CellView> elements = list({LabelID(2), LabelID(5)}).elements();

    EXPECT_FALSE(comparisonOrder(elements[0], elements[1]).has_value());
    EXPECT_FALSE(comparisonOrder(list({LabelID(2)}), list({LabelID(5)})).has_value());
}
