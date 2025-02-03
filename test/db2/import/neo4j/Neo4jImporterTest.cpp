#include <gtest/gtest.h>

#include <range/v3/view/enumerate.hpp>

#include "Graph.h"
#include "GraphView.h"
#include "TuringTest.h"
#include "GraphReader.h"
#include "GraphReport.h"
#include "DataPartBuilder.h"
#include "EdgeView.h"
#include "JobSystem.h"
#include "Neo4j/Neo4JParserConfig.h"
#include "Neo4jImporter.h"
#include "Time.h"

using namespace db;
using namespace js;
namespace rv = ranges::views;

class Neo4jImporterTest : public turing::test::TuringTest {
protected:
    void initialize() override {
        _graph = std::make_unique<Graph>();
        _jobSystem = std::make_unique<JobSystem>(1);
        _jobSystem->initialize();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph {nullptr};
};

TEST_F(Neo4jImporterTest, Simple) {
    {
        auto builder1 = _graph->newPartWriter();
        builder1->addNode(LabelSet::fromList({1})); // 0
        builder1->addNode(LabelSet::fromList({0})); // 1
        builder1->addNode(LabelSet::fromList({1})); // 2
        builder1->addNode(LabelSet::fromList({2})); // 3
        builder1->addEdge(0, 0, 1);
        builder1->commit(*_jobSystem);

        auto builder2 = _graph->newPartWriter();
        builder2->addEdge(2, 1, 2);
        EntityID id1 = builder2->addNode(LabelSet::fromList({0}));
        builder2->addNodeProperty<types::String>(id1, 0, "test1");

        builder2->addEdge(2, 0, 1);
        EntityID id2 = builder2->addNode(LabelSet::fromList({0}));
        builder2->addNodeProperty<types::String>(id2, 0, "test2");
        builder2->addNodeProperty<types::String>(2, 0, "test3");
        const EdgeRecord& edge = builder2->addEdge(4, 3, 4);
        builder2->addEdgeProperty<types::String>(edge, 0, "Edge property test");
        builder2->addNode(LabelSet::fromList({0}));
        builder2->addNode(LabelSet::fromList({0}));
        builder2->addNode(LabelSet::fromList({0}));
        builder2->addNode(LabelSet::fromList({0}));
        builder2->addNode(LabelSet::fromList({1}));
        builder2->addNode(LabelSet::fromList({1}));
        builder2->addNode(LabelSet::fromList({1}));
        builder2->addEdge(0, 3, 4);
        builder2->commit(*_jobSystem);
    }

    const auto view = _graph->view();
    const auto reader = view.read();
    std::cout << "All out edges: ";
    for (const auto& edge : reader.scanOutEdges()) {
        std::cout << edge._edgeID.getValue()
                  << edge._nodeID.getValue()
                  << edge._otherID.getValue()
                  << std::endl;
        auto view = reader.getEdgeView(edge._edgeID);
        std::cout << "  Has " << view.properties().getCount() << " properties" << std::endl;
    }
    std::cout << std::endl;

    auto it = reader.scanNodeProperties<types::String>(0).begin();
    for (; it.isValid(); it.next()) {
        std::cout << it.getCurrentNodeID().getValue() << ": " << it.get() << std::endl;
    }
}

TEST_F(Neo4jImporterTest, General) {
    auto t0 = Clock::now();
    auto t1 = Clock::now();

    const std::string turingHome = std::getenv("TURING_HOME");
    const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / "cyber-security-db";
    t0 = Clock::now();
    const bool res = Neo4jImporter::importJsonDir(*_jobSystem,
                                                  _graph.get(),
                                                  db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                                  db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                                  {
                                                      ._jsonDir = jsonDir,
                                                      ._workDir = _outDir,
                                                  });
    t1 = Clock::now();
    ASSERT_TRUE(res);
    std::cout << "Parsing: " << duration<Microseconds>(t0, t1) << " us" << std::endl;

    static std::vector<std::string_view> labelsRef = {
        "Group",
        "Domain",
        "OU",
        "User",
        "Computer",
        "GPO",
        "HighValue",
    };

    static std::vector<std::string_view> edgeTypesRef = {
        "GP_LINK",
        "CONTAINS",
        "GENERIC_ALL",
        "OWNS",
        "WRITE_OWNER",
        "WRITE_DACL",
        "DC_SYNC",
        "GET_CHANGES",
        "GET_CHANGES_ALL",
        "MEMBER_OF",
        "ADMIN_TO",
        "CAN_RDP",
        "EXECUTE_DCOM",
        "ALLOWED_TO_DELEGATE",
        "HAS_SESSION",
        "GENERIC_WRITE",
    };

    static std::vector<std::string_view> propTypesRef = {
        "isacl (Bool)",
        "enforced (Bool)",
        "name (String)",
        "objectid (String)",
        "neo4jImportId (String)",
        "domain (String)",
        "blocksInheritance (Bool)",
        "owned (Bool)",
        "enabled (Bool)",
        "pwdlastset (Int64)",
        "pwdlastset (UInt64)",
        "displayname (String)",
        "lastlogon (Int64)",
        "lastlogon (UInt64)",
        "hasspn (Bool)",
        "operatingsystem (String)",
        "highvalue (Bool)",
    };

    const auto* meta = _graph->getMetadata();
    const auto& labels = meta->labels();
    const auto& labelsets = meta->labelsets();
    const auto& edgeTypes = meta->edgeTypes();
    const auto& propTypes = meta->propTypes();

    ASSERT_EQ(labels.getCount(), labelsRef.size());
    ASSERT_EQ(labelsets.getCount(), 7);
    ASSERT_EQ(edgeTypes.getCount(), edgeTypesRef.size());
    ASSERT_EQ(propTypes.getCount(), propTypesRef.size());

    for (const auto [i, ref] : labelsRef | rv::enumerate) {
        const LabelID id = labels.get(std::string{ref});
        ASSERT_TRUE(id.isValid());
    }

    for (const auto [i, ref] : edgeTypesRef | rv::enumerate) {
        const EdgeTypeID id = edgeTypes.get(std::string{ref});
        ASSERT_TRUE(id.isValid());
    }

    for (const auto [i, ref] : propTypesRef | rv::enumerate) {
        const PropertyType pt = propTypes.get(std::string{ref});
        ASSERT_TRUE(pt._id.isValid());
    }

    const auto view = _graph->view();
    const auto reader = view.read();
    std::stringstream report;
    GraphReport::getReport(reader, report);
    std::cout << report.view() << std::endl;
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 50;
    });
}
