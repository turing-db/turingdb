#include "TuringTest.h"

#include "JobSystem.h"
#include "Graph.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "gtest/gtest.h"
#include "writers/GraphWriter.h"

using namespace turing::test;
using namespace db;

class GraphWriterTest : public TuringTest {
protected:
    void initialize() override { _jobSystem = JobSystem::create(); }

    void terminate() override { _jobSystem->terminate(); }

    std::unique_ptr<JobSystem> _jobSystem;


    [[nodiscard]] std::unique_ptr<Graph> create1Graph() {
        auto graph = Graph::create();
        GraphWriter writer {graph.get()};

        auto node = writer.addNode({"Object"});
        writer.addNodeProperty<types::String>(node, "Value", "1");
        writer.addNodeProperty<types::Int64>(node, "intValue", 1);

        writer.submit();

        return graph;
    }
};

// Testing creation of the graph with one object and no edges
TEST_F(GraphWriterTest, createSoloGraph) {
    auto graph = create1Graph();
    ASSERT_NE(graph, nullptr);

    const FrozenCommitTx transaction = graph->openTransaction();
    GraphReader reader = transaction.readGraph();
    auto parts = reader.dataparts();
    EXPECT_EQ(parts.size(), 1);
}
