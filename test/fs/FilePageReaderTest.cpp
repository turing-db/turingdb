#include "TuringTest.h"
#include "FilePageReader.h"

class FilePageReaderTest : public TuringTest {
protected:
    void initialize() override {
    }

    void terminate() override {
    }
};

TEST_F(FilePageReaderTest, HardwareChecks) {
    static const size_t systemPageSize = sysconf(_SC_PAGE_SIZE);
    ASSERT_GT(fs::FilePageReader::PAGE_SIZE, systemPageSize);
    ASSERT_TRUE(fs::FilePageReader::PAGE_SIZE % systemPageSize == 0);
    ASSERT_TRUE(fs::FilePageReader::InternalBuffer::ALIGNMENT % systemPageSize == 0);
}

