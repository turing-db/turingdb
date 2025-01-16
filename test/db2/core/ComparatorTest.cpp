#include "TuringTest.h"

#include <range/v3/view/zip.hpp>

#include "GraphReader.h"
#include "Graph.h"
#include "DataPartBuilder.h"
#include "comparators/DataPartComparator.h"
#include "JobSystem.h"

using namespace db;
namespace rv = ranges::views;

class ComparatorTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem.initialize();
    }

    void terminate() override {
        _jobSystem.terminate();
    }

    [[nodiscard]] std::unique_ptr<Graph> createDB1() {
        auto graph = std::make_unique<Graph>();

        auto* metadata = graph->getMetadata();
        auto& labels = metadata->labels();
        auto person = labels.getOrCreate("Person");
        auto officer = labels.getOrCreate("Officer");
        auto object = labels.getOrCreate("Object");

        auto& proptypes = metadata->propTypes();
        auto name = proptypes.getOrCreate("Name", ValueType::String);
        auto desc = proptypes.getOrCreate("Description", ValueType::String);
        auto age = proptypes.getOrCreate("Age", ValueType::UInt64);
        auto since = proptypes.getOrCreate("Since", ValueType::UInt64);

        auto& edgetypes = metadata->edgeTypes();
        auto has = edgetypes.getOrCreate("HasObject");
        auto knows = edgetypes.getOrCreate("Knows");

        auto change = graph->newPartWriter();
        auto p1 = change->addNode(LabelSet::fromList({person}));
        auto p2 = change->addNode(LabelSet::fromList({person, officer}));
        auto o1 = change->addNode(LabelSet::fromList({object}));
        auto o2 = change->addNode(LabelSet::fromList({object}));
        change->addNodeProperty<types::String>(p1, name._id, "John");
        change->addNodeProperty<types::String>(p2, name._id, "Jane");
        change->addNodeProperty<types::UInt64>(p1, age._id, 32);
        change->addNodeProperty<types::UInt64>(p2, age._id, 41);
        change->addNodeProperty<types::String>(o1, desc._id, "Badge");
        change->addNodeProperty<types::String>(o2, desc._id, "Uniform");

        auto h2 = change->addEdge(has, p2, o1);   // Jane has badge
        auto h3 = change->addEdge(has, p2, o2);   // Jane has uniform
        auto k1 = change->addEdge(knows, p1, p2); // John knows Jane
        auto k2 = change->addEdge(knows, p2, p1); // Jane knows John
        change->addEdgeProperty<types::UInt64>(h2, since._id, 6);
        change->addEdgeProperty<types::UInt64>(h3, since._id, 6);
        change->addEdgeProperty<types::UInt64>(k1, since._id, 3);
        change->addEdgeProperty<types::UInt64>(k2, since._id, 3);

        change->commit(_jobSystem);

        return graph;
    }

    [[nodiscard]] std::unique_ptr<Graph> createDB2() {
        auto graph = std::make_unique<Graph>();

        auto* metadata = graph->getMetadata();
        auto& labels = metadata->labels();
        auto person = labels.getOrCreate("Person");
        auto officer = labels.getOrCreate("Officer");
        auto object = labels.getOrCreate("Object");

        auto& proptypes = metadata->propTypes();
        auto name = proptypes.getOrCreate("Name", ValueType::String);
        auto desc = proptypes.getOrCreate("Description", ValueType::String);
        auto age = proptypes.getOrCreate("Age", ValueType::UInt64);
        auto since = proptypes.getOrCreate("Since", ValueType::UInt64);

        auto& edgetypes = metadata->edgeTypes();
        auto has = edgetypes.getOrCreate("HasObject");
        auto knows = edgetypes.getOrCreate("Knows");

        auto change = graph->newPartWriter();
        auto p1 = change->addNode(LabelSet::fromList({person}));
        auto p2 = change->addNode(LabelSet::fromList({person, officer}));
        auto o1 = change->addNode(LabelSet::fromList({object}));
        auto o2 = change->addNode(LabelSet::fromList({object}));
        auto o3 = change->addNode(LabelSet::fromList({object}));
        change->addNodeProperty<types::String>(p1, name._id, "John");
        change->addNodeProperty<types::String>(p2, name._id, "Jane");
        change->addNodeProperty<types::UInt64>(p1, age._id, 32);
        change->addNodeProperty<types::UInt64>(p2, age._id, 41);
        change->addNodeProperty<types::String>(o1, desc._id, "Badge");
        change->addNodeProperty<types::String>(o2, desc._id, "Uniform");
        change->addNodeProperty<types::String>(o3, desc._id, "Car");

        auto h1 = change->addEdge(has, p1, o3);   // John has car
        auto h2 = change->addEdge(has, p2, o1);   // Jane has badge
        auto h3 = change->addEdge(has, p2, o2);   // Jane has uniform
        auto h4 = change->addEdge(has, p2, o3);   // Jane has car
        auto k1 = change->addEdge(knows, p1, p2); // John knows Jane
        auto k2 = change->addEdge(knows, p2, p1); // Jane knows John
        change->addEdgeProperty<types::UInt64>(h1, since._id, 4);
        change->addEdgeProperty<types::UInt64>(h2, since._id, 6);
        change->addEdgeProperty<types::UInt64>(h3, since._id, 6);
        change->addEdgeProperty<types::UInt64>(h4, since._id, 2);
        change->addEdgeProperty<types::UInt64>(k1, since._id, 3);
        change->addEdgeProperty<types::UInt64>(k2, since._id, 3);

        change->commit(_jobSystem);

        return graph;
    }

    JobSystem _jobSystem;
};


TEST_F(ComparatorTest, Equal) {
    auto db1 = createDB1();
    auto db2 = createDB1();

    GraphReader reader1 = db1->read();
    GraphReader reader2 = db2->read();

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

    GraphReader reader1 = db1->read();
    GraphReader reader2 = db2->read();

    auto parts1 = reader1.dataparts();
    auto parts2 = reader2.dataparts();
    ASSERT_EQ(parts1.size(), parts2.size());

    for (const auto& [p1, p2] : rv::zip(parts1, parts2)) {
        ASSERT_FALSE(DataPartComparator::same(*p1, *p2));
    }
}
