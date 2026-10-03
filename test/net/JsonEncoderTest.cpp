#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "JsonEncoder.h"
#include "LocalMemory.h"
#include "QueryStatus.h"
#include "ID.h"
#include "columns/ColumnVector.h"
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

}

TEST(JsonEncoderTest, ReplacesInvalidUtf8InErrorDetails) {
    StringWriter writer;
    db::JsonEncoder<StringWriter> encoder(writer);

    encoder.start();
    encoder.encodeError(db::QueryStatus::Status::PARSE_ERROR, "CALL \xF6" "b.labels()");
    encoder.finish();

    EXPECT_EQ(writer.getOutput(), "{\"error\":\"PARSE_ERROR\",\"error_details\":\"CALL \xEF\xBF\xBD" "b.labels()\"}");
}

TEST(JsonEncoderTest, ReplacesInvalidUtf8InColumnNamesAndStrings) {
    db::LocalMemory localMem;

    auto* names = localMem.alloc<db::ColumnVector<db::types::String::Primitive>>();
    names->push_back("R\xC3\xA9my");
    names->push_back("\xF0\x9F\x98\x80");
    names->push_back("\xE2\x82");
    names->push_back("\xED\xA0\x80");
    names->push_back("\xC0\xAF");

    const std::string_view columnNames[] = {"n.name\xFF"};
    const db::Column* columns[] = {names};

    StringWriter writer;
    db::JsonEncoder<StringWriter> encoder(writer);

    encoder.start();
    encoder.writeColumnHeaders(columnNames, columns);
    encoder.writeColumns(columns, 0, names->size());
    encoder.finish();

    const std::string_view replacement = "\xEF\xBF\xBD";
    const std::string expected = "{\"header\":{\"column_names\":[\"n.name" + std::string(replacement) + "\"],"
                                 "\"column_types\":[\"String\"]},"
                                 "\"data\":[[["
                                 "\"R\xC3\xA9my\","
                                 "\"\xF0\x9F\x98\x80\","
                                 "\"" + std::string(replacement) + "\","
                                 "\"" + std::string(replacement) + std::string(replacement) + std::string(replacement) + "\","
                                 "\"" + std::string(replacement) + std::string(replacement) + "\""
                                 "]]]}";

    EXPECT_EQ(writer.getOutput(), expected);
}

TEST(JsonEncoderTest, WritesInvalidEntityIDsAsNull) {
    db::LocalMemory localMem;

    auto* nodes = localMem.alloc<db::ColumnVector<db::NodeID>>();
    nodes->push_back(db::NodeID {2});
    nodes->push_back(db::NodeID {});

    auto* edges = localMem.alloc<db::ColumnVector<db::EdgeID>>();
    edges->push_back(db::EdgeID {});
    edges->push_back(db::EdgeID {7});

    const std::string_view columnNames[] = {"c", "e"};
    const db::Column* columns[] = {nodes, edges};

    StringWriter writer;
    db::JsonEncoder<StringWriter> encoder(writer);

    encoder.start();
    encoder.writeColumnHeaders(columnNames, columns);
    encoder.writeColumns(columns, 0, nodes->size());
    encoder.finish();

    EXPECT_EQ(writer.getOutput(), "{\"header\":{\"column_names\":[\"c\",\"e\"],"
                                  "\"column_types\":[\"UInt64\",\"UInt64\"]},"
                                  "\"data\":[[[2,null],[null,7]]]}");
}
