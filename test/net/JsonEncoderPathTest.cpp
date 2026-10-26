#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "GraphPath.h"
#include "ID.h"
#include "JsonEncoder.h"
#include "LocalMemory.h"
#include "columns/ColumnVector.h"

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

// A path is its IDs in the order it runs, node, edge, node, and the null path an OPTIONAL
// MATCH leaves is null
TEST(JsonEncoderPathTest, WritesAPathAsItsIDsInOrder) {
    db::LocalMemory localMem;

    db::GraphPath twoHops;
    twoHops.start(db::NodeID(0));
    twoHops.addHop(db::EdgeID(1), db::NodeID(6));
    twoHops.addHop(db::EdgeID(4), db::NodeID(2));

    db::GraphPath noHop;
    noHop.start(db::NodeID(7));

    auto* paths = localMem.alloc<db::ColumnVector<db::GraphPath>>();
    paths->push_back(twoHops);
    paths->push_back(db::GraphPath {});
    paths->push_back(noHop);

    const std::string_view columnNames[] = {"p"};
    const db::Column* columns[] = {paths};

    StringWriter writer;
    db::JsonEncoder<StringWriter> encoder(writer);

    encoder.start();
    encoder.writeColumnHeaders(columnNames, columns);
    encoder.writeColumns(columns, 0, paths->size());
    encoder.finish();

    EXPECT_EQ(writer.getOutput(), "{\"header\":{\"column_names\":[\"p\"],"
                                  "\"column_types\":[\"Path\"]},"
                                  "\"data\":[["
                                  "[[0,1,6,4,2],null,[7]]"
                                  "]]}");
}
