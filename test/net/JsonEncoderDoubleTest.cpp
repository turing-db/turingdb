#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

#include "JsonEncoder.h"
#include "LocalMemory.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "list/ListBuffer.h"
#include "map/MapBuffer.h"
#include "metadata/PropertyType.h"

namespace {

class StringWriter {
public:
    void write(std::string_view text) { _output.append(text); }
    void write(char c) { _output.push_back(c); }

    const std::string& getOutput() const { return _output; }

private:
    std::string _output;
};

constexpr double NAN_DOUBLE = std::numeric_limits<double>::quiet_NaN();
constexpr double INFINITY_DOUBLE = std::numeric_limits<double>::infinity();
constexpr float NAN_FLOAT = std::numeric_limits<float>::quiet_NaN();
constexpr float INFINITY_FLOAT = std::numeric_limits<float>::infinity();

}

TEST(JsonEncoderDoubleTest, WritesShortestRoundTripDoubles) {
    db::LocalMemory localMem;

    auto* values = localMem.alloc<db::ColumnVector<db::types::Double::Primitive>>();
    values->push_back(3.14159265358);
    values->push_back(1.0e-9);
    values->push_back((2.0 + 3.5 + 4.5) / 3.0);
    values->push_back(0.1);
    values->push_back(-0.25);
    values->push_back(2.0);
    values->push_back(1.0e300);

    auto* optionals = localMem.alloc<db::ColumnVector<std::optional<db::types::Double::Primitive>>>();
    optionals->push_back(5.0e-324);
    optionals->push_back(std::nullopt);
    optionals->push_back(0.0);
    optionals->push_back(123456789.125);
    optionals->push_back(-1.0e-7);
    optionals->push_back(1.0e21);
    optionals->push_back(100.0);

    const std::string_view columnNames[] = {"v", "o"};
    const db::Column* columns[] = {values, optionals};

    StringWriter writer;
    db::JsonEncoder<StringWriter> encoder(writer);

    encoder.start();
    encoder.writeColumnHeaders(columnNames, columns);
    encoder.writeColumns(columns, 0, values->size());
    encoder.finish();

    EXPECT_EQ(writer.getOutput(), "{\"header\":{\"column_names\":[\"v\",\"o\"],"
                                  "\"column_types\":[\"Double\",\"Double\"]},"
                                  "\"data\":[["
                                  "[3.14159265358,1e-09,3.3333333333333335,0.1,-0.25,2.0,1e+300],"
                                  "[5e-324,null,0.0,123456789.125,-1e-07,1e+21,100.0]"
                                  "]]}");
}

TEST(JsonEncoderDoubleTest, WritesShortestRoundTripEmbeddingFloats) {
    db::LocalMemory localMem;

    const float first[] = {0.1f, 1.0f, 1.0e-9f, -3.14159265f};
    const float second[] = {0.333333343f};

    auto* embeddings = localMem.alloc<db::ColumnVector<db::types::Embedding::Primitive>>();
    embeddings->push_back(db::types::Embedding::Primitive {first});
    embeddings->push_back(db::types::Embedding::Primitive {second});

    const std::string_view columnNames[] = {"e"};
    const db::Column* columns[] = {embeddings};

    StringWriter writer;
    db::JsonEncoder<StringWriter> encoder(writer);

    encoder.start();
    encoder.writeColumnHeaders(columnNames, columns);
    encoder.writeColumns(columns, 0, embeddings->size());
    encoder.finish();

    EXPECT_EQ(writer.getOutput(), "{\"header\":{\"column_names\":[\"e\"],"
                                  "\"column_types\":[\"Embedding\"]},"
                                  "\"data\":[[[[0.1,1.0,1e-09,-3.1415927],[0.33333334]]]]}");
}

TEST(JsonEncoderDoubleTest, WritesNonFiniteDoublesAsJavaScriptTokens) {
    db::LocalMemory localMem;

    auto* values = localMem.alloc<db::ColumnVector<db::types::Double::Primitive>>();
    values->push_back(NAN_DOUBLE);
    values->push_back(-NAN_DOUBLE);
    values->push_back(INFINITY_DOUBLE);
    values->push_back(-INFINITY_DOUBLE);

    auto* optionals = localMem.alloc<db::ColumnVector<std::optional<db::types::Double::Primitive>>>();
    optionals->push_back(-NAN_DOUBLE);
    optionals->push_back(std::nullopt);
    optionals->push_back(-INFINITY_DOUBLE);
    optionals->push_back(INFINITY_DOUBLE);

    auto* constant = localMem.alloc<db::ColumnConst<db::types::Double::Primitive>>();
    constant->set(-INFINITY_DOUBLE);

    const std::string_view columnNames[] = {"v", "o", "c"};
    const db::Column* columns[] = {values, optionals, constant};

    StringWriter writer;
    db::JsonEncoder<StringWriter> encoder(writer);

    encoder.start();
    encoder.writeColumnHeaders(columnNames, columns);
    encoder.writeColumns(columns, 0, values->size());
    encoder.finish();

    EXPECT_EQ(writer.getOutput(), "{\"header\":{\"column_names\":[\"v\",\"o\",\"c\"],"
                                  "\"column_types\":[\"Double\",\"Double\",\"Double\"]},"
                                  "\"data\":[["
                                  "[NaN,NaN,Infinity,-Infinity],"
                                  "[NaN,null,-Infinity,Infinity],"
                                  "[-Infinity,-Infinity,-Infinity,-Infinity]"
                                  "]]}");
}

TEST(JsonEncoderDoubleTest, WritesNonFiniteDoublesInListsAndMapsAsJavaScriptTokens) {
    db::LocalMemory localMem;
    db::ListBuffer<> lists;
    db::MapBuffer<> maps;

    const float embedding[] = {NAN_FLOAT, -INFINITY_FLOAT};
    const db::types::Embedding::Primitive embeddingView {embedding};

    const std::array<db::ListBuffer<>::ListItemVariant, 5> items {
        1.0,
        -NAN_DOUBLE,
        INFINITY_DOUBLE,
        -INFINITY_DOUBLE,
        embeddingView,
    };

    const std::array<db::MapBuffer<>::MapKeyValuePair, 3> entries {
        db::MapBuffer<>::MapKeyValuePair {.key = "a", .value = -INFINITY_DOUBLE},
        db::MapBuffer<>::MapKeyValuePair {.key = "b", .value = NAN_DOUBLE},
        db::MapBuffer<>::MapKeyValuePair {.key = "c", .value = embeddingView},
    };

    auto* listColumn = localMem.alloc<db::ColumnVector<db::ListView>>();
    listColumn->push_back(lists.insert(items));

    auto* mapColumn = localMem.alloc<db::ColumnVector<db::MapView>>();
    mapColumn->push_back(maps.insert(entries));

    const std::string_view columnNames[] = {"l", "m"};
    const db::Column* columns[] = {listColumn, mapColumn};

    StringWriter writer;
    db::JsonEncoder<StringWriter> encoder(writer);

    encoder.start();
    encoder.writeColumnHeaders(columnNames, columns);
    encoder.writeColumns(columns, 0, listColumn->size());
    encoder.finish();

    EXPECT_EQ(writer.getOutput(), "{\"header\":{\"column_names\":[\"l\",\"m\"],"
                                  "\"column_types\":[\"List\",\"Map\"]},"
                                  "\"data\":[["
                                  "[[1.0, NaN, Infinity, -Infinity, [NaN,-Infinity]]],"
                                  "[{\"a\": -Infinity, \"b\": NaN, \"c\": [NaN,-Infinity]}]"
                                  "]]}");
}

TEST(JsonEncoderDoubleTest, WritesNonFiniteEmbeddingFloatsAsJavaScriptTokens) {
    db::LocalMemory localMem;

    const float values[] = {NAN_FLOAT, -NAN_FLOAT, INFINITY_FLOAT, -INFINITY_FLOAT, 2.0f};

    auto* embeddings = localMem.alloc<db::ColumnVector<db::types::Embedding::Primitive>>();
    embeddings->push_back(db::types::Embedding::Primitive {values});

    const std::string_view columnNames[] = {"e"};
    const db::Column* columns[] = {embeddings};

    StringWriter writer;
    db::JsonEncoder<StringWriter> encoder(writer);

    encoder.start();
    encoder.writeColumnHeaders(columnNames, columns);
    encoder.writeColumns(columns, 0, embeddings->size());
    encoder.finish();

    EXPECT_EQ(writer.getOutput(), "{\"header\":{\"column_names\":[\"e\"],"
                                  "\"column_types\":[\"Embedding\"]},"
                                  "\"data\":[[[[NaN,NaN,Infinity,-Infinity,2.0]]]]}");
}
