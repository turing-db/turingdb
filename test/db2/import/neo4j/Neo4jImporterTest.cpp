#include <gtest/gtest.h>

#include <range/v3/view/enumerate.hpp>

#include "Graph.h"
#include "GraphView.h"
#include "TuringTest.h"
#include "DataPartBuilder.h"
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
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph {nullptr};

    void testImport(size_t threadCount) {
        _jobSystem = std::make_unique<JobSystem>(threadCount);
        _jobSystem->initialize();
        const std::string turingHome = std::getenv("TURING_HOME");
        const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / "cyber-security-db";
        auto t0 = Clock::now();
        const bool res = Neo4jImporter::importJsonDir(*_jobSystem,
                                                      _graph.get(),
                                                      db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                                      db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                                      {
                                                          ._jsonDir = jsonDir,
                                                          ._workDir = _outDir,
                                                      });
        auto t1 = Clock::now();
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
            const LabelID id = labels.get(std::string {ref});
            ASSERT_TRUE(id.isValid());
        }

        for (const auto [i, ref] : edgeTypesRef | rv::enumerate) {
            const EdgeTypeID id = edgeTypes.get(std::string {ref});
            ASSERT_TRUE(id.isValid());
        }

        for (const auto [i, ref] : propTypesRef | rv::enumerate) {
            const PropertyType pt = propTypes.get(std::string {ref});
            ASSERT_TRUE(pt._id.isValid());
        }

        _jobSystem->terminate();
    }
};

TEST_F(Neo4jImporterTest, General) {
    testImport(1);
    testImport(2);
    testImport(4);
    testImport(8);
    testImport(16);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 50;
    });
}
