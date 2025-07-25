#include <cstdint>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <vector>

#include "ID.h"
#include "JobSystem.h"
#include "TuringTest.h"

#include "Graph.h"
#include "QueryInterpreter.h"
#include "TuringDB.h"
#include "SimpleGraph.h"
#include "indexes/StringIndex.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

using namespace turing::test;
using namespace ::testing;
using namespace db;

class StringIndexTest : public TuringTest {
public:
protected:
    void initialize() override { _jobSystem = JobSystem::create(); }

    void terminate() override { _jobSystem->terminate(); }

    std::unique_ptr<db::JobSystem> _jobSystem;

    [[nodiscard]] std::unique_ptr<Graph> createDB1() {
        auto graph = Graph::create();
        GraphWriter writer {graph.get()};

        auto p1 = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(p1, "Name", "Cyrus");
        writer.addNodeProperty<types::UInt64>(p1, "Age", 22);

        auto p2 = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(p2, "Name", "Suhas");
        writer.addNodeProperty<types::UInt64>(p2, "Age", 0);

        auto e1 = writer.addEdge("Colleagues", p1, p2);
        writer.addEdgeProperty<types::String>(e1, "For", "2 weeks");

        writer.submit();

        return graph;
    }

    [[nodiscard]] std::unique_ptr<Graph> createBioGraph() {
        auto graph = Graph::create();
        GraphWriter writer {graph.get()};

        auto node1 = writer.addNode({"Chemical"});
        writer.addNodeProperty<types::String>(node1, "name", "APOE-4 [extracellular]");
        auto node2 = writer.addNode({"Chemical"});
        writer.addNodeProperty<types::String>(node2, "name", "APOE4 [extracellular]");
        auto node3 = writer.addNode({"Chemical"});
        writer.addNodeProperty<types::String>(node3, "name", "APOE-4 [intracellular]");
        auto node4 = writer.addNode({"Chemical"});
        writer.addNodeProperty<types::String>(node4, "name", "APOE4 [intracellular]");

        writer.submit();

        return graph;
    }

    [[nodiscard]] std::unique_ptr<Graph> createMultiDatapartGraph() {
        auto graph = Graph::create();
        GraphWriter writer {graph.get()};

        auto poliwag = writer.addNode({"Pokemon"});
        writer.addNodeProperty<types::String>(poliwag, "name", "Poliwag");
        writer.addNodeProperty<types::String>(poliwag, "stage", "evolution stage one");

        auto sandshrew = writer.addNode({"Pokemon"});
        writer.addNodeProperty<types::String>(sandshrew, "name", "Sandshrew");
        writer.addNodeProperty<types::String>(sandshrew, "stage", "evolution stage one");

        writer.commit();

        auto poliwhirl = writer.addNode({"Pokemon"});
        writer.addNodeProperty<types::String>(poliwhirl, "name", "Poliwhirl");
        writer.addNodeProperty<types::String>(poliwhirl, "stage", "evolution stage two");

        auto sandslash = writer.addNode({"Pokemon"});
        writer.addNodeProperty<types::String>(sandslash, "name", "Sandslash");
        writer.addNodeProperty<types::String>(sandslash, "stage", "evolution stage two");

        writer.commit();

        auto poliwrath = writer.addNode({"Pokemon"});
        writer.addNodeProperty<types::String>(poliwrath, "name", "Poliwrath");
        writer.addNodeProperty<types::String>(poliwrath, "stage",
                                              "evolution stage three");

        writer.submit();

        return graph;
    }
};

TEST_F(StringIndexTest, simpleIndex) {
    using StringPropertyIndex =
        std::unordered_map<PropertyTypeID, std::unique_ptr<StringIndex>>;
    constexpr uint64_t FIRSTNODEID = 0;
    constexpr uint64_t SECONDNODEID = 1;
    constexpr uint64_t FIRSTEDGEID = 0;

    auto db2 = createDB1();
    const FrozenCommitTx transaction1 = db2->openTransaction();
    GraphReader reader1 = transaction1.readGraph();
    auto parts1 = reader1.dataparts();

    ASSERT_EQ(1, parts1.size());
    auto dpIt = parts1.begin();
    auto datapart = dpIt->get();

    const StringPropertyIndex& nodePropIdx = datapart->getNodeStrPropIndex();
    // We only have a single node string property
    ASSERT_EQ(1, nodePropIdx.size());

    // Get the prefix trie corresponding to the "name" property
    auto& nameIdx = nodePropIdx.at(0);

    std::vector<EntityID> owners1 {};
    nameIdx->query(owners1, "Cyrus");
    ASSERT_EQ(1, owners1.size());
    EXPECT_EQ(FIRSTNODEID, owners1.at(0));

    std::vector<EntityID> owners2 {};
    nameIdx->query(owners2, "Suhas");
    ASSERT_EQ(1, owners2.size());
    EXPECT_EQ(SECONDNODEID, owners2.at(0));

    std::vector<EntityID> owners3 {};
    nameIdx->query(owners3, "Remy");
    EXPECT_EQ(0, owners3.size());

    std::vector<EntityID> owners4 {};
    nameIdx->query(owners4, "Cy");
    ASSERT_EQ(1, owners4.size());
    EXPECT_EQ(FIRSTNODEID, owners4.at(0));

    const StringPropertyIndex& edgePropIdx = datapart->getEdgeStrPropIndex();
    ASSERT_EQ(1, edgePropIdx.size());

    // Get the prefix trie corresponding to the "For" property
    auto& [propId2, forIdx] = *edgePropIdx.cbegin();

    std::vector<EntityID> owners5 {};
    forIdx->query(owners5, "2 weeks");
    ASSERT_EQ(1, owners5.size());
    EXPECT_EQ(FIRSTEDGEID, owners5.at(0));

    std::vector<EntityID> owners6 {};
    forIdx->query(owners6, "2");
    ASSERT_EQ(1, owners6.size());
    EXPECT_EQ(FIRSTEDGEID, owners6.at(0));

    std::vector<EntityID> owners7 {};
    forIdx->query(owners7, "weeks");
    ASSERT_EQ(1, owners7.size());
    EXPECT_EQ(FIRSTEDGEID, owners7.at(0));

    std::vector<EntityID> owners8 {};
    forIdx->query(owners8, "we");
    ASSERT_EQ(1, owners8.size());
    EXPECT_EQ(FIRSTEDGEID, owners8.at(0));

    std::vector<EntityID> owners9 {};
    forIdx->query(owners9, "eeks");
    EXPECT_EQ(0, owners9.size());

    std::vector<EntityID> owners10 {};
    forIdx->query(owners10, "10 years");
    EXPECT_EQ(0, owners10.size());
}


TEST_F(StringIndexTest, stringApproximation) {
    using StringPropertyIndex =
        std::unordered_map<PropertyTypeID, std::unique_ptr<StringIndex>>;

    auto db = createBioGraph();
    const FrozenCommitTx transaction = db->openTransaction();
    GraphReader reader = transaction.readGraph();
    auto parts = reader.dataparts();

    ASSERT_EQ(1, parts.size());
    auto dpIt = parts.begin();
    auto datapart = dpIt->get();

    const StringPropertyIndex& nodePropIdx = datapart->getNodeStrPropIndex();
    // We only have a single node string property
    ASSERT_EQ(1, nodePropIdx.size());

    constexpr size_t NAMEPROPID = 0;

    // Get the prefix trie corresponding to the "name" property
    auto& nameIdx = nodePropIdx.at(NAMEPROPID);

    std::vector<EntityID> owners {};
    nameIdx->query(owners, "APOE");
    EXPECT_THAT(owners, UnorderedElementsAre(0, 1, 2, 3)) << "Query for 'APOE' failed";

    owners.clear();
    nameIdx->query(owners, "apoe");
    EXPECT_THAT(owners, UnorderedElementsAre(0, 1, 2, 3)) << "Query for 'apoe' failed";

    owners.clear();
    nameIdx->query(owners, "APOE-4");
    EXPECT_THAT(owners, UnorderedElementsAre(0, 1, 2, 3)) << "Query for 'APOE-4' failed";

    owners.clear();
    nameIdx->query(owners, "APOE4");
    EXPECT_THAT(owners, UnorderedElementsAre(1, 3)) << "Query for 'APOE4' failed";

    owners.clear();
    nameIdx->query(owners, "APOE 4");
    EXPECT_THAT(owners, UnorderedElementsAre(0, 1, 2, 3)) << "Query for 'APOE 4' failed";

    owners.clear();
    nameIdx->query(owners, "APO");
    EXPECT_THAT(owners, UnorderedElementsAre(0, 1, 2, 3)) << "Query for 'APO' failed";

    owners.clear();
    nameIdx->query(owners, "intra");
    EXPECT_THAT(owners, UnorderedElementsAre(2, 3)) << "Query for 'intra' failed";

    owners.clear();
    nameIdx->query(owners, "extra");
    EXPECT_THAT(owners, UnorderedElementsAre(0, 1)) << "Query for 'extra' failed";
}


TEST_F(StringIndexTest, multiDataPartTest) {
    auto db = createMultiDatapartGraph();
    const FrozenCommitTx transaction = db->openTransaction();
    GraphReader reader = transaction.readGraph();
    auto parts = reader.dataparts();

    ASSERT_EQ(parts.size(), 3);

    const auto PWAGNODEID = NodeID(0);
    const auto PWHIRLNODEID = NodeID(2);
    const auto PWRATHNODEID = NodeID(4);

    const auto SANDSHREWNODEID = NodeID(1);
    const auto SANDSLASHNODEID = NodeID(3);

    auto datapartIterator = parts.begin();

    // First stage/datapart
    auto& firstStageIndex = datapartIterator->get()->getNodeStrPropIndex();
    constexpr size_t NAMEPROPERTYOFFSET = 0;
    auto& fstPropertyIndex = firstStageIndex.at(NAMEPROPERTYOFFSET);

    {
        std::vector<NodeID> owners {};
        fstPropertyIndex->query<NodeID>(owners, "poli");
        EXPECT_THAT(owners, UnorderedElementsAre(PWAGNODEID))
            << "Query for 'poli' failed";
    }

    {
        std::vector<NodeID> owners {};
        fstPropertyIndex->query<NodeID>(owners, "sand");
        EXPECT_THAT(owners, UnorderedElementsAre(SANDSHREWNODEID))
            << "Query for 'sand' failed";
    }

    datapartIterator++;

    // Second stage/datapart
    auto& secondStageIndex = datapartIterator->get()->getNodeStrPropIndex();
    auto& sndPropertyIndex = secondStageIndex.at(NAMEPROPERTYOFFSET);


    {
        std::vector<NodeID> owners {};
        sndPropertyIndex->query<NodeID>(owners, "poli");
        EXPECT_THAT(owners, UnorderedElementsAre(PWHIRLNODEID))
            << "Query for 'poli' failed";
    }

    {
        std::vector<NodeID> owners {};
        sndPropertyIndex->query<NodeID>(owners, "sand");
        EXPECT_THAT(owners, UnorderedElementsAre(SANDSLASHNODEID))
            << "Query for 'poli' failed";
    }

    datapartIterator++;

    // Third stage/datapart
    auto& thirdStageIndex = datapartIterator->get()->getNodeStrPropIndex();
    auto& thdPropertyIndex = thirdStageIndex.at(NAMEPROPERTYOFFSET);

    {
        std::vector<NodeID> owners {};
        thdPropertyIndex->query<NodeID>(owners, "poli");
        EXPECT_THAT(owners, UnorderedElementsAre(PWRATHNODEID))
            << "Query for 'poli' failed";
    }
}
