#include "TuringTest.h"

#include <range/v3/view/zip.hpp>

#include "Graph.h"
#include "reader/GraphReader.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "writers/DataPartBuilder.h"
#include "comparators/DataPartComparator.h"
#include "JobSystem.h"
#include "writers/GraphWriter.h"

using namespace turing::test;
using namespace db;

namespace rv = ranges::views;

class ComparatorTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = JobSystem::create();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    [[nodiscard]] std::unique_ptr<Graph> createDB1() {
        auto graph = Graph::create();
        GraphWriter writer {graph.get()};

        auto p1 = writer.addNode({"Person"});
        auto p2 = writer.addNode({ "Person", "Officer" });
        auto o1 = writer.addNode({"Object"});
        auto o2 = writer.addNode({"Object"});
        writer.addNodeProperty<types::String>(p1, "Name", "John");
        writer.addNodeProperty<types::String>(p2, "Name", "Jane");
        writer.addNodeProperty<types::UInt64>(p1, "Age", 32);
        writer.addNodeProperty<types::UInt64>(p2, "Age", 41);
        writer.addNodeProperty<types::String>(o1, "Description", "Badge");
        writer.addNodeProperty<types::String>(o2, "Description", "Uniform");

        auto h2 = writer.addEdge("HasObject", p2, o1);   // Jane has badge
        auto h3 = writer.addEdge("HasObject", p2, o2);   // Jane has uniform
        auto k1 = writer.addEdge("Knows", p1, p2); // John knows Jane
        auto k2 = writer.addEdge("Knows", p2, p1); // Jane knows John
        writer.addEdgeProperty<types::UInt64>(h2, "Since", 6);
        writer.addEdgeProperty<types::UInt64>(h3, "Since", 6);
        writer.addEdgeProperty<types::UInt64>(k1, "Since", 3);
        writer.addEdgeProperty<types::UInt64>(k2, "Since", 3);

        writer.commit();

        return graph;
    }

    [[nodiscard]] std::unique_ptr<Graph> createDB2() {
        auto graph = Graph::create();
        GraphWriter writer {graph.get()};

        auto p1 = writer.addNode({"Person"});
        auto p2 = writer.addNode({ "Person", "Officer" });
        auto o1 = writer.addNode({"Object"});
        auto o2 = writer.addNode({"Object"});
        auto o3 = writer.addNode({"Object"});
        writer.addNodeProperty<types::String>(p1, "Name", "John");
        writer.addNodeProperty<types::String>(p2, "Name", "Jane");
        writer.addNodeProperty<types::UInt64>(p1, "Age", 32);
        writer.addNodeProperty<types::UInt64>(p2, "Age", 41);
        writer.addNodeProperty<types::String>(o1, "Description", "Badge");
        writer.addNodeProperty<types::String>(o2, "Description", "Uniform");
        writer.addNodeProperty<types::String>(o3, "Description", "Car");

        auto h1 = writer.addEdge("HasObject", p1, o3);   // John has car
        auto h2 = writer.addEdge("HasObject", p2, o1);   // Jane has badge
        auto h3 = writer.addEdge("HasObject", p2, o2);   // Jane has uniform
        auto h4 = writer.addEdge("HasObject", p2, o3);   // Jane has car
        auto k1 = writer.addEdge("Knows", p1, p2); // John knows Jane
        auto k2 = writer.addEdge("Knows", p2, p1); // Jane knows John

        writer.addEdgeProperty<types::UInt64>(h1, "Since", 4);
        writer.addEdgeProperty<types::UInt64>(h2, "Since", 6);
        writer.addEdgeProperty<types::UInt64>(h3, "Since", 6);
        writer.addEdgeProperty<types::UInt64>(h4, "Since", 2);
        writer.addEdgeProperty<types::UInt64>(k1, "Since", 3);
        writer.addEdgeProperty<types::UInt64>(k2, "Since", 3);

        writer.commit();

        return graph;
    }

    std::unique_ptr<db::JobSystem> _jobSystem;
};


TEST_F(ComparatorTest, Equal) {
    auto db1 = createDB1();
    auto db2 = createDB1();


    const Transaction transaction1 = db1->openTransaction();
    const Transaction transaction2 = db2->openTransaction();
    GraphReader reader1 = transaction1.readGraph();
    GraphReader reader2 = transaction2.readGraph();

    auto parts1 = reader1.dataparts();
    auto parts2 = reader2.dataparts();
    ASSERT_EQ(parts1.size(), parts2.size());

    for (const auto& [p1, p2] : rv::zip(parts1, parts2)) {
        ASSERT_TRUE(DataPartComparator::same(*p1, *p2));
    }
}

TEST_F(ComparatorTest, NotEqual) {
    auto db1 = createDB1();
    auto db2 = createDB2();

    const Transaction transaction1 = db1->openTransaction();
    const Transaction transaction2 = db2->openTransaction();
    GraphReader reader1 = transaction1.readGraph();
    GraphReader reader2 = transaction2.readGraph();

    auto parts1 = reader1.dataparts();
    auto parts2 = reader2.dataparts();
    ASSERT_EQ(parts1.size(), parts2.size());

    for (const auto& [p1, p2] : rv::zip(parts1, parts2)) {
        ASSERT_FALSE(DataPartComparator::same(*p1, *p2));
    }
}
