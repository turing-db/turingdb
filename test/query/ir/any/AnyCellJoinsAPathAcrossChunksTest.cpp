#include <gtest/gtest.h>

#include <stddef.h>

#include <span>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr size_t PATH_COUNT = 3000;
constexpr size_t CHUNK_SIZE = 128;

class ChunkCountingSink : public RowSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _chunkCount++;
        RowSink::appendChunks(chunks, offset, rowCount);
    }

    size_t getChunkCount() const { return _chunkCount; }

private:
    size_t _chunkCount {0};
};

void renderAppendedPath(const Row& row, size_t firstID, bool pathFirst, std::string& list) {
    const std::string path = "(" + row[firstID] + "), [" + row[firstID + 1] + "], (" + row[firstID + 2] + ")";
    list = pathFirst ? "[" + path + ", 1]" : "[1, " + path + "]";
}

}

// A list a cell holds keeps a path appended to it as a view of the path's IDs. 3,000 paths at
// 128 rows a chunk cross 24 chunks, and a sort or a collect holds the lists of every chunk
// before the first is read.
class AnyCellJoinsAPathAcrossChunksTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("UNWIND range(1, " + std::to_string(PATH_COUNT) + ") AS i CREATE (:P {i: i})-[:R]->(:Q {i: i})");
        _interpreter->setChunkSize(CHUNK_SIZE);
    }

    void readRows(std::string_view query, Rows& rows) {
        ChunkCountingSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        ASSERT_GT(sink.getChunkCount(), 1u) << query;

        rows = sink.rows();
    }

    // A row reads the IDs of a path's start node, edge and end node from @param firstID on
    void expectEveryPathAppended(const Rows& rows, size_t firstID, bool pathFirst) {
        ASSERT_EQ(rows.size(), PATH_COUNT);

        for (const Row& row : rows) {
            std::string expected;
            renderAppendedPath(row, firstID, pathFirst, expected);

            ASSERT_EQ(row.back(), expected) << "row of " << row.front();
        }
    }
};

TEST_F(AnyCellJoinsAPathAcrossChunksTest, appendsThePathOfEveryRowASortHolds) {
    Rows rows;
    readRows("MATCH p = (a:P)-[r:R]->(b:Q) RETURN a.i, id(a), id(r), id(b), [[1], 'a'][0] + p ORDER BY a.i", rows);

    expectEveryPathAppended(rows, 1, false);

    for (size_t rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
        ASSERT_EQ(rows[rowIndex].front(), std::to_string(rowIndex + 1));
    }
}

TEST_F(AnyCellJoinsAPathAcrossChunksTest, appendsThePathOfEveryRowASortMoved) {
    Rows rows;
    readRows("MATCH p = (a:P)-[r:R]->(b:Q) WITH a, r, b, p ORDER BY a.i DESC "
             "RETURN a.i, id(a), id(r), id(b), p + [[1], 'a'][0]",
             rows);

    expectEveryPathAppended(rows, 1, true);
}

TEST_F(AnyCellJoinsAPathAcrossChunksTest, appendsThePathOfEveryRowACollectHolds) {
    Rows rows;
    readRows("MATCH p = (a:P)-[r:R]->(b:Q) WITH a, r, b, [[1], 'a'][0] + p AS l "
             "WITH collect([id(a), id(r), id(b), l]) AS paths UNWIND paths AS path "
             "RETURN path[0], path[1], path[2], path[3]",
             rows);

    expectEveryPathAppended(rows, 0, false);
}
