#include "TuringTest.h"

#include "File.h"
#include "FilePageReader.h"

class FilePageReaderTest : public TuringTest {
protected:
    void initialize() override {
        _testfilePath = fs::Path {_outDir + "/testfile"};
    }

    void terminate() override {
    }

    void writeOnes(size_t count) {
        std::vector<uint64_t> ones(count, 1);
        auto f = fs::File::createAndOpen(_testfilePath);
        ASSERT_TRUE(f.has_value());
        f->clearContent();
        f->write(ones.data(), ones.size() * sizeof(uint64_t));
        f->close();
    }

    void testOnes(size_t onesCount) {
        writeOnes(onesCount); // 2 pages
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

        ASSERT_EQ(sum, onesCount);
    }

    fs::Path _testfilePath;
};

TEST_F(FilePageReaderTest, HardwareChecks) {
    static const size_t systemPageSize = sysconf(_SC_PAGE_SIZE);
    ASSERT_GT(fs::FilePageReader::PAGE_SIZE, systemPageSize);
    ASSERT_TRUE(fs::FilePageReader::PAGE_SIZE % systemPageSize == 0);
    ASSERT_TRUE(fs::FilePageReader::InternalBuffer::ALIGNMENT % systemPageSize == 0);
}

TEST_F(FilePageReaderTest, Pages) {
    testOnes(7);
    testOnes(512);
    testOnes(1024);
    testOnes(1024ul * 1024); // Exactly one page (1MiB * sizeof(uint64_t))
    testOnes(1024ul * 1024 + 7);
    testOnes(1024ul * 1024 * 2); // Exactly two pages (2MiB * sizeof(uint64_t))
    testOnes(1024ul * 1024 * 2 + 7);
}

