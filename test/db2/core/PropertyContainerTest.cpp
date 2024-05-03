#include "BioLog.h"
#include "FileUtils.h"
#include "NodeContainer.h"
#include "NodePropertyView.h"
#include "PerfStat.h"
#include "PropertyView.h"
#include "TestNodes.h"
#include "TimerStat.h"
#include "EdgeRecord.h"

#include <gtest/gtest.h>

using namespace db;
using namespace std;

using Clock = std::chrono::high_resolution_clock;
float getMilliseconds(auto& t0, auto& t1) {
    return std::chrono::duration<float, std::milli>(t1 - t0).count();
}

using namespace chrono;

TEST(PropertyContainerTest, NodeProperties) {
    const testing::TestInfo* const testInfo =
        testing::UnitTest::GetInstance()->current_test_info();

    srand(0);
    std::string outDir;
    FileUtils::Path logPath;
    outDir = testInfo->test_suite_name();
    outDir += "_";
    outDir += testInfo->name();
    outDir += ".out";
    logPath = FileUtils::Path(outDir) / "log";

    if (FileUtils::exists(outDir)) {
        FileUtils::removeDirectory(outDir);
    }
    FileUtils::createDirectory(outDir);

    Log::BioLog::init();
    Log::BioLog::openFile(logPath.string());
    PerfStat::init(outDir + "/perf");

    NodeContainer::Builder nodesBuilder;
    NodePropertyContainer::Builder::NodeCountsPerLabelset nodeCounts;
    NodePropertyContainer::Builder builder;

    std::vector<test_nodes::TestNode> nodes;
    auto propertyTypeInfos = test_nodes::PROPERTY_TYPE_INFOS;

    constexpr size_t multiple = 1;
    size_t nodeCount = test_nodes::NODES.size() * multiple;
    nodes.resize(nodeCount);

    for (size_t i = 0; i < nodeCount; i++) {
        const size_t offset = i % test_nodes::NODES.size();
        nodes[i] = test_nodes::NODES[offset];
        nodes[i]._id = i;
    }

    for (auto& info : propertyTypeInfos) {
        info.second._count *= multiple;
    }

    for (const auto& node : nodes) {
        nodeCounts[node._labelset]++;
    }

    nodesBuilder.initialize(0, nodes.size(), nodeCounts);
    builder.initialize(0, nodes.size(), propertyTypeInfos, nodeCounts);

    for (auto& n : nodes) {
        TempNodeData tmpNodeData {
            ._labelset = n._labelset};
        nodesBuilder.addNode(tmpNodeData, n._id);
    }

    // nodesBuilder.addNode( )
    for (const auto& n : nodes) {
        for (const auto& prop : n._props) {
            const auto& info = propertyTypeInfos.at(prop._typeID);
            switch (info._type) {
                case ValueType::String: {
                    builder.addProperty<types::String>(
                        prop._typeID,
                        nodesBuilder.getNodePositionFromTempID(n._id),
                        std::get<std::string>(prop._value));
                    break;
                }
                case ValueType::Bool: {
                    builder.addProperty<types::Bool>(
                        prop._typeID,
                        nodesBuilder.getNodePositionFromTempID(n._id),
                        std::get<bool>(prop._value));
                    break;
                }
                case ValueType::UInt64: {
                    builder.addProperty<types::UInt64>(
                        prop._typeID,
                        nodesBuilder.getNodePositionFromTempID(n._id),
                        std::get<uint64_t>(prop._value));
                    break;
                }
                case ValueType::Double: {
                    builder.addProperty<types::Double>(
                        prop._typeID,
                        nodesBuilder.getNodePositionFromTempID(n._id),
                        std::get<double>(prop._value));
                    break;
                }
                case ValueType::Int64: {
                    break;
                }
                case ValueType::Invalid:
                case ValueType::_SIZE: {
                    throw;
                }
            }
        }
    }

    std::unique_ptr<NodePropertyContainer> container = builder.build();

    {
        std::string output;
        std::cout << "Property type infos:" << std::endl;
        for (const auto& [ptID, ptType] : propertyTypeInfos) {
            output += "ID: " + std::to_string(ptID)
                    + "; Count: " + std::to_string(ptType._count)
                    + "; Importance: " + std::to_string((size_t)container->getImportance(ptID))
                    + "\n";
        }
        std::cout << output << std::endl;
    }

    constexpr size_t repeatMeasure = 1;
    // Regex search on mandatory string
    size_t calcCount = 0;
    size_t matchCount = 0;
    std::string pattern = "EGFR";
    auto t0 = std::chrono::high_resolution_clock::now();
    TimerStat* timer1 = new TimerStat("Mandatory strings");

    auto mandatorySpan = container->getAllMandatoryProperties<types::String>(1);
    for (size_t i = 0; i < repeatMeasure; i++) {
        for (const auto& v : mandatorySpan) {
            if (v.find(pattern) != std::string::npos) {
                matchCount++;
            }
            calcCount++;
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    delete timer1;

    float durMandatory = duration_cast<nanoseconds>(t1 - t0).count();
    std::cout << "MANDATORY: Matched " << matchCount
              << " out of " << calcCount
              << " strings: " << durMandatory / 1000.0f << " us" << std::endl;

    // Search on mandatory ints
    calcCount = 0;
    matchCount = 0;
    TimerStat* timer2 = new TimerStat("Mandatory Ints");
    t0 = std::chrono::high_resolution_clock::now();

    auto mandatoryIntSpan = container->getAllMandatoryProperties<types::UInt64>(0);
    for (size_t i = 0; i < repeatMeasure; i++) {
        for (const auto& v : mandatoryIntSpan) {
            if (v > 564476) {
                matchCount++;
            }
            calcCount++;
        }
    }
    t1 = std::chrono::high_resolution_clock::now();
    delete timer2;

    float durIntMandatory = duration_cast<nanoseconds>(t1 - t0).count();
    std::cout << "MANDATORY: Matched " << matchCount
              << " out of " << calcCount
              << " integers: " << durIntMandatory / 1000.0f << " us" << std::endl;

    // Search on optional string (51/52 entries)
    // This is basically displayName copy pasted to obtain an optional
    // property for better comparison with the mandatory prop
    calcCount = 0;
    matchCount = 0;

    TimerStat* timer3 = new TimerStat("Optional strings");
    t0 = std::chrono::high_resolution_clock::now();
    auto optionalSpan = container->getAllOptionalProperties<types::String>(12);
    for (size_t i = 0; i < repeatMeasure; i++) {
        for (const auto& v : optionalSpan) {
            if (v.has_value()) {
                if (v.value().find(pattern) != std::string::npos) {
                    matchCount++;
                }
                calcCount++;
            }
        }
    }
    t1 = std::chrono::high_resolution_clock::now();
    delete timer3;

    float durOptional = duration_cast<nanoseconds>(t1 - t0).count();

    std::cout << "OPTIONAL: Matched " << matchCount
              << " out of " << calcCount
              << " strings: " << durOptional / 1000.0f << " us" << std::endl;

    t0 = std::chrono::high_resolution_clock::now();
    size_t totalPropCount = 0;
    for (const test_nodes::TestNode& n : nodes) {
        const EntityPropertyView view = container->getNodePropertyView(n._id);
        totalPropCount += view.getPropertyCount();
    }
    ASSERT_TRUE(totalPropCount != 0);
    t1 = std::chrono::high_resolution_clock::now();
    float dur = duration_cast<nanoseconds>(t1 - t0).count();
    std::cout << "EntityPropertyView construct duration: "
              << dur / 1000.0f << " us" << std::endl;

    t0 = std::chrono::high_resolution_clock::now();
    size_t count = 0;
    for (const test_nodes::TestNode& n : nodes) {
        const EntityPropertyView view = container->getEntityPropertyView(n._id);
        if (view.hasProperty(3)) {
            count++;
        }
    }
    ASSERT_EQ(count, test_nodes::PROPERTY_TYPE_INFOS.at(3)._count * multiple);
    t1 = std::chrono::high_resolution_clock::now();
    dur = duration_cast<nanoseconds>(t1 - t0).count();
    std::cout << "NodePropertyView::hasProperty: "
              << dur / 1000.0f << " us" << std::endl;

    {
        // Node 16 is APOE4 [extracellular region] (Homo sapiens)
        NodePropertyView propertyView = container->getNodePropertyView(16);

        std::unordered_map<PropertyTypeID, db::test_nodes::TestProperty> theoProps = {
            {0,  {0, (uint64_t)9711070}                    },
            {1,  {1, (std::string) "APOE-4 [extracellula"} },
            {2,  {2, false}                                },
            {3,  {3, (std::string) "ReferenceGeneProduct"} },
            {4,  {4, (std::string) "EntityWithAccessione"} },
            {5,  {5, (std::string) "Homo sapiens"}         },
            {6,  {6, (std::string) "R-HSA-9711070"}        },
            {7,  {7, (std::string) "R-HSA-9711070.1"}      },
            {12, {12, (std::string) "APOE-4 [extracellula"}},
        };
        ASSERT_EQ(theoProps.size(), propertyView.getPropertyCount());

        const auto& displayName = propertyView.getProperty<types::String>(1);
        ASSERT_STREQ(displayName.c_str(), "APOE-4 [extracellula");

        const auto& speciesName = propertyView.getProperty<types::String>(5);
        ASSERT_STREQ(speciesName.c_str(), "Homo sapiens");

        // All properties check
        for (const auto& v : propertyView) {
            ASSERT_TRUE(theoProps.find(v._id) != theoProps.end());
        }

        // String properties check
        for (const auto& v : propertyView.strings()) {
            ASSERT_TRUE(theoProps.find(v._id) != theoProps.end());
            const auto& theo = std::get<std::string>(theoProps.at(v._id)._value);
            const auto& prop = std::get<const types::String::Primitive*>(v._value);
            ASSERT_STREQ(theo.c_str(), prop->c_str());
        }

        // Bool properties check
        for (const auto& v : propertyView.bools()) {
            ASSERT_TRUE(theoProps.find(v._id) != theoProps.end());
            const auto& theo = std::get<bool>(theoProps.at(v._id)._value);
            const auto& prop = std::get<const types::Bool::Primitive*>(v._value);
            ASSERT_EQ(theo, prop->_boolean);
        }

        // Double properties check
        for (const auto& v : propertyView.doubles()) {
            ASSERT_TRUE(theoProps.find(v._id) != theoProps.end());
            const auto& theo = std::get<double>(theoProps.at(v._id)._value);
            const auto& prop = std::get<const types::Double::Primitive*>(v._value);
            ASSERT_EQ(theo, *prop);
        }

        // UInt64 properties check
        for (const auto& v : propertyView.uint64s()) {
            ASSERT_TRUE(theoProps.find(v._id) != theoProps.end());
            const auto& theo = std::get<uint64_t>(theoProps.at(v._id)._value);
            const auto& prop = std::get<const types::UInt64::Primitive*>(v._value);
            ASSERT_EQ(theo, *prop);
        }
    }

    Log::BioLog::destroy();
    PerfStat::destroy();
}
