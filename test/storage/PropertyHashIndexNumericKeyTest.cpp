#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <vector>

#include "Graph.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "indexes/PropertyHashIndex.h"
#include "list/ListBuffer.h"
#include "list/ListContainer.h"
#include "list/ListView.h"
#include "map/MapBuffer.h"
#include "map/MapContainer.h"
#include "map/MapView.h"
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

#include "JobSystem.h"

using namespace db;

namespace {

using MapEntry = MapBuffer<>::MapKeyValuePair;
using ListItem = ListBuffer<>::ListItemVariant;

}

// The index answers the same question Cypher's = does, so a key holding a number matches
// one holding the same number under another numeric type.
class PropertyHashIndexNumericKeyTest : public testing::Test {
protected:
    void SetUp() override {
        _graph = Graph::create();

        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(_graph.get(), &jobSystem);

        MapContainer maps;
        ListContainer lists;

        const NodeID node = writer.addNode({"Item"});

        const std::vector<MapEntry> entries = {{"a", types::Int64::Primitive {1}}};
        writer.addNodeProperty<types::Map>(node, "attrs", maps.insert(entries));

        const std::vector<ListItem> elements = {types::Int64::Primitive {1}};
        writer.addNodeProperty<types::List>(node, "tags", lists.insert(elements));

        writer.submit();
    }

    template <SupportedType T>
    size_t countMatches(std::string_view propertyName, typename T::Primitive key) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        const std::optional<PropertyType> propertyType = view.metadata().propTypes().get(propertyName);
        EXPECT_TRUE(propertyType.has_value());

        PropertyHashIndex<T, NodeID> index("index", propertyType->_id);
        index.init(view);

        ColumnConst<typename T::Primitive> input;
        input.set(key);

        ColumnVector<NodeID> result;
        index.query(&input, &result);

        return result.size();
    }

    std::unique_ptr<Graph> _graph;
};

TEST_F(PropertyHashIndexNumericKeyTest, findsAMapByItsOwnKey) {
    MapBuffer<> buffer;
    const std::vector<MapEntry> entries = {{"a", types::Int64::Primitive {1}}};

    EXPECT_EQ(countMatches<types::Map>("attrs", buffer.insert(entries)), 1);
}

TEST_F(PropertyHashIndexNumericKeyTest, findsAMapHoldingAnIntegerByAnEqualDouble) {
    MapBuffer<> buffer;
    const std::vector<MapEntry> entries = {{"a", types::Double::Primitive {1.0}}};

    EXPECT_EQ(countMatches<types::Map>("attrs", buffer.insert(entries)), 1);
}

TEST_F(PropertyHashIndexNumericKeyTest, doesNotFindAMapByAnotherNumber) {
    MapBuffer<> buffer;
    const std::vector<MapEntry> entries = {{"a", types::Double::Primitive {1.5}}};

    EXPECT_EQ(countMatches<types::Map>("attrs", buffer.insert(entries)), 0);
}

TEST_F(PropertyHashIndexNumericKeyTest, findsAListHoldingAnIntegerByAnEqualDouble) {
    ListBuffer<> buffer;
    const std::vector<ListItem> elements = {types::Double::Primitive {1.0}};

    EXPECT_EQ(countMatches<types::List>("tags", buffer.insert(elements)), 1);
}
