#include "PropertyContainer.h"
#include "BioLog.h"
#include "FileUtils.h"
#include "NodePropertyView.h"
#include "PerfStat.h"
#include "PropertyView.h"
#include "TestNodes.h"
#include "TimerStat.h"

#include <gtest/gtest.h>

using namespace db;
using namespace std;
using namespace chrono;

TEST(PropertyContainerTest, Create) {
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

    PropertyContainer::Builder::NodeCountsPerLabelSet nodeCounts;
    PropertyContainer::Builder builder;

    std::vector<test_nodes::TestNode> nodes;
    auto propertyTypeInfos = test_nodes::PROPERTY_TYPE_INFOS;

    constexpr size_t multiple = 42000;
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

    builder.initialize(0,
                       nodes.size(),
                       propertyTypeInfos,
                       nodeCounts);

    for (auto& n : nodes) {
        for (const auto& prop : n._props) {
            const auto& info = propertyTypeInfos.at(prop._typeID);
            switch (info._type) {
                case ValueType::String: {
                    builder.setNextProp<StringPropertyType>(
                        prop._typeID,
                        n._labelset,
                        std::get<std::string>(prop._value));
                    break;
                }
                case ValueType::Bool: {
                    builder.setNextProp<BoolPropertyType>(
                        prop._typeID,
                        n._labelset,
                        std::get<bool>(prop._value));
                    break;
                }
                case ValueType::UInt64: {
                    builder.setNextProp<UInt64PropertyType>(
                        prop._typeID,
                        n._labelset,
                        std::get<uint64_t>(prop._value));
                    break;
                }
                case ValueType::Double: {
                    builder.setNextProp<DoublePropertyType>(
                        prop._typeID,
                        n._labelset,
                        std::get<double>(prop._value));
                    break;
                }
                case ValueType::Int64: {
                    break;
                }
                case ValueType::_SIZE: {
                    throw;
                }
            }
        }
        n._id = builder.finishNode(n._labelset);
    }

    std::unique_ptr<PropertyContainer> container = builder.build();

    {
        std::string output;
        std::cout << "Property type infos:" << std::endl;
        for (const auto& [ptID, ptType] : propertyTypeInfos) {
            output += "ID: " + std::to_string(ptID.getID())
                    + "; Count: " + std::to_string(ptType._count)
                    + "; Importance: " + std::to_string((size_t)container->getImportance(ptID))
                    + "\n";
        }
        std::cout << output << std::endl;
        // ASSERT_STREQ(output.c_str(),
        //              "ID: 0; Count: 52; Importance: 0\n"
        //              "ID: 1; Count: 52; Importance: 0\n"
        //              "ID: 2; Count: 49; Importance: 1\n"
        //              "ID: 3; Count: 18; Importance: 1\n"
        //              "ID: 4; Count: 52; Importance: 0\n"
        //              "ID: 5; Count: 49; Importance: 1\n"
        //              "ID: 6; Count: 49; Importance: 1\n"
        //              "ID: 7; Count: 49; Importance: 1\n"
        //              "ID: 8; Count: 19; Importance: 1\n"
        //              "ID: 9; Count: 9; Importance: 1\n"
        //              "ID: 10; Count: 10; Importance: 1\n"
        //              "ID: 11; Count: 10; Importance: 1\n"
        //              "ID: 12; Count: 51; Importance: 1\n");
    }

    constexpr size_t repeatMeasure = 1;
    // Regex search on mandatory string
    size_t calcCount = 0;
    size_t matchCount = 0;
    std::string pattern = "EGFR";
    auto t0 = std::chrono::high_resolution_clock::now();
    TimerStat* timer1 = new TimerStat("Mandatory strings");

    auto mandatorySpan = container->getAllMandatoryProperties<StringPropertyType>(1);
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

    auto mandatoryIntSpan = container->getAllMandatoryProperties<UInt64PropertyType>(0);
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
    auto optionalSpan = container->getAllOptionalProperties<StringPropertyType>(12);
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
        const NodePropertyView view = container->getNodePropertyView(n._id);
        totalPropCount += view.getPropertyCount();
    }
    ASSERT_TRUE(totalPropCount != 0);
    t1 = std::chrono::high_resolution_clock::now();
    float dur = duration_cast<nanoseconds>(t1 - t0).count();
    std::cout << "NodePropertyView construct duration: "
              << dur / 1000.0f << " us" << std::endl;

    t0 = std::chrono::high_resolution_clock::now();
    size_t count = 0;
    for (const test_nodes::TestNode& n : nodes) {
        const NodePropertyView view = container->getNodePropertyView(n._id);
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

        const auto& displayName = propertyView.getProperty<StringPropertyType>(1);
        ASSERT_STREQ(displayName.c_str(), "APOE-4 [extracellula");

        const auto& speciesName = propertyView.getProperty<StringPropertyType>(5);
        ASSERT_STREQ(speciesName.c_str(), "Homo sapiens");

        // All properties check
        for (const auto& v : propertyView) {
            ASSERT_TRUE(theoProps.find(v._id) != theoProps.end());
        }

        // String properties check
        for (const auto& v : propertyView.strings()) {
            ASSERT_TRUE(theoProps.find(v._id) != theoProps.end());
            const auto& theo = std::get<std::string>(theoProps.at(v._id)._value);
            const auto& prop = std::get<const StringPropertyType::Primitive*>(v._value);
            ASSERT_STREQ(theo.c_str(), prop->c_str());
        }

        // Bool properties check
        for (const auto& v : propertyView.bools()) {
            ASSERT_TRUE(theoProps.find(v._id) != theoProps.end());
            const auto& theo = std::get<bool>(theoProps.at(v._id)._value);
            const auto& prop = std::get<const BoolPropertyType::Primitive*>(v._value);
            ASSERT_EQ(theo, prop->_boolean);
        }

        // Double properties check
        for (const auto& v : propertyView.doubles()) {
            ASSERT_TRUE(theoProps.find(v._id) != theoProps.end());
            const auto& theo = std::get<double>(theoProps.at(v._id)._value);
            const auto& prop = std::get<const DoublePropertyType::Primitive*>(v._value);
            ASSERT_EQ(theo, *prop);
        }

        // UInt64 properties check
        for (const auto& v : propertyView.uint64s()) {
            ASSERT_TRUE(theoProps.find(v._id) != theoProps.end());
            const auto& theo = std::get<uint64_t>(theoProps.at(v._id)._value);
            const auto& prop = std::get<const UInt64PropertyType::Primitive*>(v._value);
            ASSERT_EQ(theo, *prop);
        }
    }

    Log::BioLog::destroy();
    PerfStat::destroy();
}
