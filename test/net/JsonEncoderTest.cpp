#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "JsonEncoder.h"
#include "LocalMemory.h"
#include "QueryStatus.h"
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
