#include <gtest/gtest.h>

#include <optional>
#include <string_view>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "iterators/GetPropertiesWithNullIterator.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

using namespace db;
using namespace turing::test;

class GetPropertiesWithNullChunkWriterTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void getPropertyTypeID(const GraphView& view, std::string_view name, PropertyTypeID& id) {
        const std::optional<PropertyType> propertyType = view.metadata().propTypes().get(name);
        ASSERT_TRUE(propertyType.has_value()) << "no property type " << name;
        id = propertyType->_id;
    }

    std::unique_ptr<Graph> _graph;
};

TEST_F(GetPropertiesWithNullChunkWriterTest, propertyOnEveryNodeYieldsNoMask) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    PropertyTypeID nameType;
    ASSERT_NO_FATAL_FAILURE(getPropertyTypeID(view, "name", nameType));

    ColumnNodeIDs input;
    for (size_t node = 0; node < 18; ++node) {
        input.push_back(NodeID {node});
    }

    ColumnOptVector<types::String::Primitive> output;
    GetNodePropertiesWithNullChunkWriter<types::String> writer(view, nameType, &input);
    writer.setOutput(&output);

    std::vector<std::string_view> names;
    while (writer.isValid()) {
        writer.fill(5);
        EXPECT_FALSE(output.hasMask());
        EXPECT_EQ(output.getNullCount(), 0u);
        for (size_t row = 0; row < output.size(); ++row) {
            names.push_back(output.getValue(row));
        }
    }

    ASSERT_EQ(names.size(), 18u);
    EXPECT_EQ(names[0], "Remy");
    EXPECT_EQ(names[1], "Adam");
    EXPECT_EQ(names[2], "Computers");
    EXPECT_EQ(names[17], "Doruk");
}

TEST_F(GetPropertiesWithNullChunkWriterTest, missingValuesAreNull) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    PropertyTypeID ageType;
    ASSERT_NO_FATAL_FAILURE(getPropertyTypeID(view, "age", ageType));

    ColumnNodeIDs input;
    input.push_back(NodeID {2});
    input.push_back(NodeID {0});
    input.push_back(NodeID {8});
    input.push_back(NodeID {1});

    ColumnOptVector<types::Int64::Primitive> output;
    GetNodePropertiesWithNullChunkWriter<types::Int64> writer(view, ageType, &input);
    writer.setOutput(&output);
    writer.fill(input.size());

    ASSERT_EQ(output.size(), 4u);
    ASSERT_TRUE(output.hasMask());
    EXPECT_EQ(output.getNullCount(), 2u);
    EXPECT_FALSE(output.isValid(0));
    EXPECT_EQ(output.get(1), std::optional<int64_t> {32});
    EXPECT_FALSE(output.isValid(2));
    EXPECT_EQ(output.get(3), std::optional<int64_t> {32});
    EXPECT_EQ(output.getValues()[0], 0);
    EXPECT_EQ(output.getValues()[2], 0);
}

TEST_F(GetPropertiesWithNullChunkWriterTest, chunkWithoutNullsAfterChunkWithNullsYieldsNoMask) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    PropertyTypeID ageType;
    ASSERT_NO_FATAL_FAILURE(getPropertyTypeID(view, "age", ageType));

    ColumnNodeIDs input;
    input.push_back(NodeID {2});
    input.push_back(NodeID {3});
    input.push_back(NodeID {0});
    input.push_back(NodeID {1});

    ColumnOptVector<types::Int64::Primitive> output;
    GetNodePropertiesWithNullChunkWriter<types::Int64> writer(view, ageType, &input);
    writer.setOutput(&output);

    writer.fill(2);
    ASSERT_EQ(output.size(), 2u);
    EXPECT_EQ(output.getNullCount(), 2u);

    writer.fill(2);
    ASSERT_EQ(output.size(), 2u);
    EXPECT_FALSE(output.hasMask());
    EXPECT_EQ(output.getNullCount(), 0u);
    EXPECT_EQ(output.getValue(0), 32);
    EXPECT_EQ(output.getValue(1), 32);
    EXPECT_FALSE(writer.isValid());
}
