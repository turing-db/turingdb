#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "GraphPath.h"
#include "ID.h"
#include "JsonEncoder.h"
#include "LocalMemory.h"
#include "PathView.h"
#include "columns/ColumnVector.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"

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

// A path a list holds is written as a path column's is, so a list of paths is an array of
// arrays of IDs
TEST(JsonEncoderPathTest, WritesTheIDsOfThePathsAListHolds) {
    db::LocalMemory localMem;

    const std::vector<uint64_t> oneHop {0, 0, 1};
    const std::vector<uint64_t> noHop {7};

    const std::vector<db::ListBuffer<>::ListItemVariant> elements {
        db::PathView {oneHop},
        db::PathView {noHop},
    };

    auto* lists = localMem.alloc<db::ColumnVector<db::ListView>>();
    lists->push_back(localMem.listBuffer().insert(elements));

    const std::string_view columnNames[] = {"paths"};
    const db::Column* columns[] = {lists};

    StringWriter writer;
    db::JsonEncoder<StringWriter> encoder(writer);

    encoder.start();
    encoder.writeColumnHeaders(columnNames, columns);
    encoder.writeColumns(columns, 0, lists->size());
    encoder.finish();

    EXPECT_EQ(writer.getOutput(), "{\"header\":{\"column_names\":[\"paths\"],"
                                  "\"column_types\":[\"List\"]},"
                                  "\"data\":[["
                                  "[[[0,0,1], [7]]]"
                                  "]]}");
}
