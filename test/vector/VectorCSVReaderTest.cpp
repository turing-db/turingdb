#include "TuringTest.h"

#include <stddef.h>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "Path.h"

#include "BatchVectorCreate.h"
#include "VectorCSVReader.h"
#include "VectorException.h"

using namespace vec;
using namespace turing::test;

// The file format LOAD VECTOR reads: one `id,dim1,...,dimN` line per vector, with the
// dimension fixed by the batch rather than by the file. Every way a file can disagree
// with that is malformed user input, which the reader parses by hand to report as a
// VectorException rather than let a std::invalid_argument escape the query.
class VectorCSVReaderTest : public TuringTest {
public:
    void initialize() override {
        _dataDir = fs::Path {_outDir} / "vectors";

        if (!_dataDir.exists()) {
            ASSERT_TRUE(_dataDir.mkdir());
        }
    }

protected:
    static constexpr Dimension dimension = 4;

    fs::Path writeFile(std::string_view name, std::string_view contents) {
        const fs::Path path = _dataDir / name;

        std::ofstream file(path.get());
        file << contents;
        file.close();

        return path;
    }

    // A null router leaves every point in one shard, so the batch's first entry holds
    // them all in file order.
    void read(const fs::Path& path, BatchVectorCreate& batch) {
        batch.init(nullptr, dimension);
        VectorCSVReader::read(path, batch);
    }

    fs::Path _dataDir;
};

TEST_F(VectorCSVReaderTest, readsOneVectorPerLine) {
    const fs::Path path = writeFile("vectors.csv",
                                    "7,1,0,0,0\n"
                                    "8,0,2.5,0,0\n"
                                    "9,0,0,-3,0.5\n");

    BatchVectorCreate batch;
    ASSERT_NO_THROW(read(path, batch));

    EXPECT_EQ(batch.count(), 3u);

    ASSERT_NE(batch.begin(), batch.end());
    const BatchVectorCreate::Data& data = *batch.begin();

    const std::vector<int64_t> expectedIDs {7, 8, 9};
    EXPECT_EQ(data._externalIDs, expectedIDs);

    const std::vector<float> expectedEmbeddings {1.0f, 0.0f, 0.0f, 0.0f,
                                                 0.0f, 2.5f, 0.0f, 0.0f,
                                                 0.0f, 0.0f, -3.0f, 0.5f};
    EXPECT_EQ(data._embeddings, expectedEmbeddings);
}

TEST_F(VectorCSVReaderTest, readsALastLineWithNoTrailingNewline) {
    const fs::Path path = writeFile("no_newline.csv", "1,1,1,1,1");

    BatchVectorCreate batch;
    ASSERT_NO_THROW(read(path, batch));

    EXPECT_EQ(batch.count(), 1u);
}

TEST_F(VectorCSVReaderTest, skipsBlankLines) {
    const fs::Path path = writeFile("blanks.csv",
                                    "\n"
                                    "1,1,1,1,1\n"
                                    "\n"
                                    "2,2,2,2,2\n"
                                    "\n");

    BatchVectorCreate batch;
    ASSERT_NO_THROW(read(path, batch));

    EXPECT_EQ(batch.count(), 2u);
}

TEST_F(VectorCSVReaderTest, readsAnEmptyFileAsNoVector) {
    const fs::Path path = writeFile("empty.csv", "");

    BatchVectorCreate batch;
    ASSERT_NO_THROW(read(path, batch));

    EXPECT_EQ(batch.count(), 0u);
}

TEST_F(VectorCSVReaderTest, missingFileThrows) {
    BatchVectorCreate batch;

    EXPECT_THROW(read(_dataDir / "absent.csv", batch), VectorException);
}

TEST_F(VectorCSVReaderTest, nonNumericIDThrows) {
    const fs::Path path = writeFile("bad_id.csv", "abc,1,1,1,1\n");

    BatchVectorCreate batch;
    EXPECT_THROW(read(path, batch), VectorException);
}

TEST_F(VectorCSVReaderTest, emptyIDThrows) {
    const fs::Path path = writeFile("no_id.csv", ",1,1,1,1\n");

    BatchVectorCreate batch;
    EXPECT_THROW(read(path, batch), VectorException);
}

TEST_F(VectorCSVReaderTest, outOfRangeIDThrows) {
    const fs::Path path = writeFile("huge_id.csv", "99999999999999999999999,1,1,1,1\n");

    BatchVectorCreate batch;
    EXPECT_THROW(read(path, batch), VectorException);
}

TEST_F(VectorCSVReaderTest, nonNumericValueThrows) {
    const fs::Path path = writeFile("bad_value.csv", "1,1,nope,1,1\n");

    BatchVectorCreate batch;
    EXPECT_THROW(read(path, batch), VectorException);
}

TEST_F(VectorCSVReaderTest, tooFewValuesThrows) {
    const fs::Path path = writeFile("short.csv", "1,1,1,1\n");

    BatchVectorCreate batch;
    EXPECT_THROW(read(path, batch), VectorException);
}

TEST_F(VectorCSVReaderTest, tooManyValuesThrows) {
    const fs::Path path = writeFile("long.csv", "1,1,1,1,1,1\n");

    BatchVectorCreate batch;
    EXPECT_THROW(read(path, batch), VectorException);
}

TEST_F(VectorCSVReaderTest, idWithNoValuesThrows) {
    const fs::Path path = writeFile("id_only.csv", "1\n");

    BatchVectorCreate batch;
    EXPECT_THROW(read(path, batch), VectorException);
}

TEST_F(VectorCSVReaderTest, malformedNumberThrowsATuringException) {
    const fs::Path path = writeFile("turing.csv", "1,1,x,1,1\n");

    BatchVectorCreate batch;
    EXPECT_THROW(read(path, batch), TuringException);
}

// A malformed line stops the read, so the batch keeps only the lines before it and
// the caller never adds a partially-read file.
TEST_F(VectorCSVReaderTest, stopsAtTheFirstMalformedLine) {
    const fs::Path path = writeFile("partial.csv",
                                    "1,1,1,1,1\n"
                                    "2,1,1,1\n"
                                    "3,1,1,1,1\n");

    BatchVectorCreate batch;
    EXPECT_THROW(read(path, batch), VectorException);
    EXPECT_EQ(batch.count(), 1u);
}
