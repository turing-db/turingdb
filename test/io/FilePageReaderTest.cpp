#include "TuringTest.h"

#include "File.h"
#include "FilePageReader.h"

using namespace turing::test;

class FilePageReaderTest : public TuringTest {
protected:
    void initialize() override {
        _testfilePath = fs::Path {_outDir + "/testfile"};
    }

    void terminate() override {
    }

    void writeOnes(size_t byteCount) {
        const size_t count = byteCount / sizeof(uint64_t);
        const size_t toWrite = count * sizeof(uint64_t);
        _ones.resize(count, 1);
        auto f = fs::File::createAndOpen(_testfilePath);
        ASSERT_TRUE(f.has_value());
        f->clearContent();
        f->write(_ones.data(), toWrite);
        f->close();
    }

    void testOnes(size_t byteCount) {
        const size_t count = byteCount / sizeof(uint64_t);
        writeOnes(byteCount);
        auto reader = fs::FilePageReader::open(_testfilePath);
        ASSERT_TRUE(reader.has_value());

        size_t sum = 0;
        for (;;) {
            reader->nextPage();
            ASSERT_FALSE(reader->errorOccured());

            for (auto it = reader->begin(); it != reader->end();) {
                sum += it.get<uint64_t>();
            }

            if (reader->reachedEnd()) {
                break;
            }
        }

        ASSERT_EQ(sum, count);
    }

    fs::Path _testfilePath;
    std::vector<uint64_t> _ones;
};

TEST_F(FilePageReaderTest, HardwareChecks) {
    static const size_t systemPageSize = sysconf(_SC_PAGE_SIZE);
    ASSERT_GT(fs::DEFAULT_PAGE_SIZE, systemPageSize);
    ASSERT_TRUE(fs::DEFAULT_PAGE_SIZE % systemPageSize == 0);
    ASSERT_TRUE(fs::AlignedBuffer::ALIGNMENT % systemPageSize == 0);
}

TEST_F(FilePageReaderTest, Pages) {
    testOnes(13);
    testOnes(1024);
    testOnes(fs::DEFAULT_PAGE_SIZE);
    testOnes(fs::DEFAULT_PAGE_SIZE + 13);
    testOnes(fs::DEFAULT_PAGE_SIZE * 2);
    testOnes(fs::DEFAULT_PAGE_SIZE * 2 + 13);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 2;
    });
}
