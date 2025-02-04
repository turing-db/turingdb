#include <gtest/gtest.h>

#include <range/v3/view/enumerate.hpp>

#include "Neo4j/NodePropertyParser.h"
#include "Graph.h"
#include "GraphView.h"
#include "TuringTest.h"
#include "GraphMetadata.h"
#include "GraphReport.h"
#include "Time.h"
#include "fs/File.h"
#include "fs/FileReader.h"
#include "fs/Path.h"

using namespace db;
using namespace js;
namespace rv = ranges::views;

class NodePropertyParserTest : public turing::test::TuringTest {
protected:
    void initialize() override {
        _metadata = std::make_unique<GraphMetadata>();
    }

    void terminate() override {
    }

    std::unique_ptr<GraphMetadata> _metadata {nullptr};
};

TEST_F(NodePropertyParserTest, General) {
    const std::string turingHome = std::getenv("TURING_HOME");
    const fs::Path jsonDir = fs::Path {turingHome} / "neo4j" / "stackoverflow-db";
    const fs::Path jsonFilePath = jsonDir / "nodeProperties.json";
    const fs::File jsonFile = fs::File::open(jsonFilePath).value();

    fs::FileReader reader;
    reader.setFile(&jsonFile);
    reader.read();
    ASSERT_FALSE(reader.errorOccured());
    auto it = reader.iterateBuffer();
    const std::string_view content = it.get<char>(reader.getBuffer().size());

    auto t0 = Clock::now();

    spdlog::info("Retrieving node properties");
    auto parser = json::neo4j::NodePropertyParser(_metadata.get());
    ASSERT_TRUE(nlohmann::json::sax_parse(content, &parser,
                                          json::neo4j::json::input_format_t::json,
                                          true, true));
    auto t1 = Clock::now();
    std::cout << "Parsing: " << duration<Microseconds>(t0, t1) << " us" << std::endl;

    static std::vector<std::string_view> propTypesRef = {
        "uuid (Int64)",
        "uuid (UInt64)",
        "uuid (String)",
        "display_name (String)",
        "name (String)",
        "link (String)",
        "title (String)",
        "is_accepted (Bool)",
        "body_markdown (String)",
        "score (Int64)",
        "score (UInt64)",
        "creation_date (Int64)",
        "creation_date (UInt64)",
        "accepted_answer_id (Int64)",
        "accepted_answer_id (UInt64)",
        "view_count (Int64)",
        "view_count (UInt64)",
        "answer_count (Int64)",
        "answer_count (UInt64)",
    };

    static std::vector<ValueType> valueTypesRef = {
        ValueType::Int64,
        ValueType::UInt64,
        ValueType::String,
        ValueType::String,
        ValueType::String,
        ValueType::String,
        ValueType::String,
        ValueType::Bool,
        ValueType::String,
        ValueType::Int64,
        ValueType::UInt64,
        ValueType::Int64,
        ValueType::UInt64,
        ValueType::Int64,
        ValueType::UInt64,
        ValueType::Int64,
        ValueType::UInt64,
        ValueType::Int64,
        ValueType::UInt64,
    };

    const auto& propTypes = _metadata->propTypes();
    ASSERT_EQ(propTypesRef.size(), propTypes.getCount());

    for (const auto [i, ref] : propTypesRef | rv::enumerate) {
        const PropertyType pt = propTypes.get(std::string {ref});
        ASSERT_TRUE(pt._id.isValid());
        ASSERT_EQ(valueTypesRef[i], pt._valueType);
    }
}
