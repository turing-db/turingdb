#include "TuringTest.h"
#include "GMLParser.h"

using namespace db;
using namespace turing::test;

class GMLSax {
public:
    bool onNodeProperty(std::string_view k, std::string_view v) {
        spdlog::info("NODEPROP[{}, {}]", k, v);
        return true;
    }
    bool onNodeProperty(std::string_view k, uint64_t v) {
        spdlog::info("NODEPROP[{}, {}]", k, v);
        return true;
    }
    bool onNodeProperty(std::string_view k, int64_t v) {
        spdlog::info("NODEPROP[{}, {}]", k, v);
        return true;
    }
    bool onNodeProperty(std::string_view k, double v) {
        spdlog::info("NODEPROP[{}, {}]", k, v);
        return true;
    }
    bool onNodeID(uint64_t id) {
        spdlog::info("NODEID[{}]", id);
        return true;
    }
    bool onEdgeProperty(std::string_view k, std::string_view v) {
        spdlog::info("EDGEPROP[{}, {}]", k, v);
        return true;
    }
    bool onEdgeProperty(std::string_view k, uint64_t v) {
        spdlog::info("EDGEPROP[{}, {}]", k, v);
        return true;
    }
    bool onEdgeProperty(std::string_view k, int64_t v) {
        spdlog::info("EDGEPROP[{}, {}]", k, v);
        return true;
    }
    bool onEdgeProperty(std::string_view k, double v) {
        spdlog::info("EDGEPROP[{}, {}]", k, v);
        return true;
    }
    bool onEdgeSource(uint64_t id) {
        spdlog::info("EDGESOURCE[{}]", id);
        return true;
    }
    bool onEdgeTarget(uint64_t id) {
        spdlog::info("EDGETARGET[{}]", id);
        return true;
    }
    bool onNodeBegin() {
        spdlog::info("NodeBegin >>>");
        return true;
    }
    bool onNodeEnd() {
        spdlog::info("<<< NodeEnd");
        return true;
    }
    bool onEdgeBegin() {
        spdlog::info("EdgeBegin >>>");
        return true;
    }
    bool onEdgeEnd() {
        spdlog::info("<<< EdgeEnd");
        return true;
    }
};

class GMLParserTest : public TuringTest {
protected:
    void initialize() override {
    }

    void terminate() override {
    }
};

#define EXPECT_ERROR(data, errMsg)                             \
    {                                                          \
        using namespace std::literals;                         \
        spdlog::info("Parsing...");                            \
        GMLSax sax;                                            \
        GMLParser<GMLSax> parser(sax);                         \
        auto res = parser.parse(data);                         \
        ASSERT_FALSE(res);                                     \
        const auto& err = res.error();                         \
        ASSERT_EQ(err.getMessage(), errMsg##sv);               \
        spdlog::info("[Expected error] {}", err.getMessage()); \
    }

#define EXPECT_SUCCESS(data)                                                 \
    {                                                                        \
        using namespace std::literals;                                       \
        GMLSax sax;                                                          \
        GMLParser<GMLSax> parser(sax);                                       \
        auto res = parser.parse(data##sv);                                   \
        if (!res)                                                            \
            spdlog::info("[UNEXPECTED ERROR] {}", res.error().getMessage()); \
        ASSERT_TRUE(res);                                                    \
    }

TEST_F(GMLParserTest, Empty) {
    EXPECT_ERROR("", "GML Error at line 1: Unexpected end of file. Expected: 'graph'");
    EXPECT_ERROR("         \n\n\r", "GML Error at line 3: Unexpected end of file. Expected: 'graph'");
    EXPECT_SUCCESS("graph []");
    EXPECT_SUCCESS("graph[]");
    EXPECT_SUCCESS(" graph []");
    EXPECT_SUCCESS(" graph [] ");
    EXPECT_SUCCESS("\tgraph [] ");
    EXPECT_SUCCESS("\tgraph [] \n");
    EXPECT_SUCCESS("\ngraph [] \n");
    EXPECT_SUCCESS("\n\n\r graph [] \n");

    EXPECT_ERROR("graphRANDOM [] dsqd\n",
                 "GML Error at line 1: Unexpected token 'graphRANDOM'. Expected: 'graph'");

    EXPECT_ERROR("\n\n\r graph [] dsqd\n",
                 "GML Error at line 3: Unexpected token 'd'. Expected: 'end of file'");

    EXPECT_ERROR("\tdsqd [] \n",
                 "GML Error at line 1: Unexpected token 'dsqd'. Expected: 'graph'");

    {
        GMLSax sax;
        GMLParser<GMLSax> parser(sax);
        auto res = parser.parse("\n\ndsqd [] \n");
        ASSERT_FALSE(res);
        const auto& err = res.error();
        spdlog::info("[Expected error] {}", err.getMessage());
        ASSERT_EQ(err.getLine(), 3);
    }
}

TEST_F(GMLParserTest, Nodes) {
    // Missing node ID + Missing closing bracket
    EXPECT_ERROR(
        "graph [\n"
        "  node [\n"
        "]",
        "GML Error at line 3: Unexpected token ']'. Expected: 'node ID'");

    // Missing node ID
    EXPECT_ERROR(
        "graph [\n"
        "  node []\n"
        "]",
        "GML Error at line 3: Unexpected token ']'. Expected: 'node ID'");

    // Missing closing bracket
    EXPECT_ERROR(
        "graph [\n"
        "  node [ id 0 \n"
        "]",
        "GML Error at line 3: Unexpected end of file. Expected: ']'");

    // Entity name not starting with alphabet
    EXPECT_ERROR(
        "graph [\n"
        "  9node [ id 0 ]\n"
        "]",
        "GML Error at line 2: Unexpected token '9'. Expected: 'alphabet'");

    EXPECT_SUCCESS(
        "graph [\n"
        "  node [ id 0 ]\n"
        "]");

    EXPECT_SUCCESS(
        "graph [\n"
        "  node [ id 0 identifier 1]\n"
        "]");

    EXPECT_SUCCESS(
        "graph [\n"
        "  node [\n"
        "    id 0\n"
        "  ]\n"
        "]");
    EXPECT_SUCCESS(
        "graph [\n"
        "  node [id 1]\n"
        "]");
    EXPECT_SUCCESS(
        "graph [\n"
        "  node [id 1 label test]\n"
        "]");
    EXPECT_SUCCESS(
        "graph [\n"
        "  node [id 1 label test ]\n"
        "]");
    EXPECT_SUCCESS(
        "graph [\n"
        "  node [id 1 label \"quoted prop\"]\n"
        "]");
    EXPECT_SUCCESS(
        "graph [\n"
        "  node [id 1 label \"quoted prop\" ]\n"
        "]");
    EXPECT_SUCCESS(
        "graph [\n"
        "  node [id 0 label \"quoted prop\"] \n"
        "  node [id 1 label test]\n"
        "]");
    EXPECT_SUCCESS(
        "graph [\n"
        "  node [id 0 label \"quoted [with bracketsprop]\"] \n"
        "  node [id 1 label test]\n"
        "]");
    EXPECT_SUCCESS(
        "graph[node[id 0 label node1]node[id 1 label node2]]");

    // Missing property value
    EXPECT_ERROR(
        "graph [\n"
        "  node [ id 0 label ]\n"
        "]",
        "GML Error at line 2: Unexpected token ']'. Expected: 'property value'");

    EXPECT_ERROR(
        "graph [\n"
        "  node               \n"
        " ",
        "GML Error at line 3: Unexpected end of file. Expected: '['");
}

TEST_F(GMLParserTest, Edges) {
    // Missing source ID
    EXPECT_ERROR(
        "graph [\n"
        "  edge []\n"
        "]",
        "GML Error at line 3: Unexpected token ']'. Expected: 'edge source ID'");

    // Missing closing bracket
    EXPECT_ERROR(
        "graph [\n"
        "  edge [ \n"
        "]",
        "GML Error at line 3: Unexpected token ']'. Expected: 'edge source ID'");

    // Missing source ID
    EXPECT_ERROR(
        "graph [\n"
        "  edge [ target 1 ]\n"
        "]",
        "GML Error at line 3: Unexpected token ']'. Expected: 'edge source ID'");

    // Missing source ID
    EXPECT_ERROR(
        "graph [\n"
        "  edge [ source 1 ]\n"
        "]",
        "GML Error at line 3: Unexpected token ']'. Expected: 'edge target ID'");

    EXPECT_SUCCESS(
        "graph [\n"
        "  node [ id 0 ] node [id 1] edge [ source 0 target 1 label edgeName] \n"
        "]");

    EXPECT_SUCCESS(
        "graph [\n"
        "  node [ id 0 ] node [id 1] edge [ source 0 target 1 label source_Test 4 target_test 6 edgeName] \n"
        "]");

    EXPECT_SUCCESS(
        "graph[node[id 0]node[id 1]edge[source 0 target 1]edge[source 1 target 0]]");
}


TEST_F(GMLParserTest, NestedProperties) {
    EXPECT_SUCCESS(
        "graph [\n"
        "  node[id 0 label \"My node\" attr[key0 value0 key1[key2 value2]key3 value3]key4 value4]\n"
        "]");

    EXPECT_SUCCESS(
        "graph [\n"
        "  node[id 0 label \"My node0\" attr[key0 value0 key1[key2 value2]key3 value3]key4 value4]\n"
        "  node[id 1 label \"My node1\" attr[key0 value0 key1[key2 value2]key3 value3]key4 value4]\n"
        "  edge[source 0 target 1 label \"My edge0\" attr[key0 value0 key1[key2 value2]key3 value3]key4 value4]\n"
        "  edge[source 1 target 0 label \"My edge1\" attr[key0 value0 key1[key2 value2]key3 value3]key4 value4]\n"
        "]");


    // Missing bracket (first edge after value3)
    EXPECT_ERROR(
        "graph [\n"
        "  node[id 0 label \"My node0\" attr[key0 value0 key1[key2 value2]key3 value3]key4 value4]\n"
        "  node[id 1 label \"My node1\" attr[key0 value0 key1[key2 value2]key3 value3]key4 value4]\n"
        "  edge[source 0 target 1 label \"My edge0\" attr[key0 value0 key1[key2 value2]key3 value3 key4 value4]\n"
        "  edge[source 1 target 0 label \"My edge1\" attr[key0 value0 key1[key2 value2]key3 value3]key4 value4]\n"
        "]",
        "GML Error at line 6: Unexpected end of file. Expected: ']'");
}
TEST_F(GMLParserTest, GraphAttributes) {
    EXPECT_SUCCESS(
        "graph [\n"
        "  attribute_1 value_1\n"
        "  attribute_2 value_2\n"
        "  node [ id 0 identifier 1]\n"
        "]");

    EXPECT_SUCCESS(
        "graph [\n"
        "  attribute_1 \"value_1\"\n"
        "  attribute_2 value_2\n"
        "  node [ id 0 identifier 1]\n"
        "]");

    EXPECT_SUCCESS(
        "graph [attribute_1 value_1 node[id 1 ]]");

    EXPECT_SUCCESS(
        "graph [\n"
        "  node_name_1 value_1\n"
        "  node [ id 0 identifier 1]\n"
        "]");


    // Can't have attribute (entity) starting with a number
    EXPECT_ERROR(
        "graph [\n"
        "  1_attribute value_1\n"
        "  node [\n"
        "]",
        "GML Error at line 2: Unexpected token '1'. Expected: 'alphabet'");

    // Shouldn't have brackets on attribute value
    EXPECT_ERROR(
        "graph [\n"
        "  attribute_1 value_1\n"
        "  attribute_2 [value_2]\n"
        "  node [ id 0 identifier 1]\n"
        "]",
        "GML Error at line 3: Unexpected token '['. Expected: 'alphabet'");
}
