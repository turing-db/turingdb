#include <stdint.h>
#include <string.h>

#include <fstream>
#include <iterator>
#include <span>
#include <string_view>
#include <vector>

#include "TuringTest.h"

#include "AlignedBuffer.h"
#include "File.h"
#include "FilePageWriter.h"
#include "FileWriter.h"
#include "Path.h"

using namespace turing::test;

namespace {

constexpr size_t PAGE_SIZE = 16384;
constexpr uint64_t VALUE = 42;

}

class EmptyNullRangeWriteTest : public TuringTest {
protected:
    void initialize() override {
        _path = fs::Path {_outDir + "/written"};
    }

    void readWritten(std::vector<uint8_t>& bytes) {
        std::ifstream file(_path.c_str(), std::ios::binary);
        bytes.assign(std::istreambuf_iterator<char> {file}, std::istreambuf_iterator<char> {});
    }

    const std::vector<uint8_t> _empty;
    fs::Path _path;
};

TEST_F(EmptyNullRangeWriteTest, AlignedBufferWritesNothing) {
    ASSERT_EQ(_empty.data(), nullptr);

    fs::AlignedBuffer buffer;
    buffer.write(_empty.data(), _empty.size());

    EXPECT_EQ(buffer.size(), 0);
}

TEST_F(EmptyNullRangeWriteTest, AlignedBufferPatchesNothing) {
    fs::AlignedBuffer buffer;
    buffer.write(reinterpret_cast<const uint8_t*>(&VALUE), sizeof(VALUE));
    buffer.patch(_empty.data(), _empty.size(), 0);

    uint64_t value {0};
    memcpy(&value, buffer.data(), sizeof(value));

    EXPECT_EQ(buffer.size(), sizeof(VALUE));
    EXPECT_EQ(value, VALUE);
}

TEST_F(EmptyNullRangeWriteTest, FilePageWriterWritesAnEmptyPayload) {
    {
        auto writer = fs::FilePageWriter::open(_path, PAGE_SIZE);
        ASSERT_TRUE(writer.has_value());

        writer->writeToCurrentPage(std::span<const uint8_t> {_empty});
        writer->writeToCurrentPage(VALUE);
        writer->finish();
        ASSERT_FALSE(writer->errorOccured());
    }

    std::vector<uint8_t> bytes;
    readWritten(bytes);
    ASSERT_EQ(bytes.size(), PAGE_SIZE);

    uint64_t value {0};
    memcpy(&value, bytes.data(), sizeof(value));
    EXPECT_EQ(value, VALUE);
}

TEST_F(EmptyNullRangeWriteTest, FileWriterWritesAnEmptyStringView) {
    {
        auto file = fs::File::createAndOpen(_path);
        ASSERT_TRUE(file.has_value());

        fs::FileWriter<64> writer;
        writer.setFile(&file.value());
        writer.write(std::string_view {});
        writer.write(std::string_view {"ab"});
        writer.flush();
        ASSERT_FALSE(writer.errorOccured());
    }

    std::vector<uint8_t> bytes;
    readWritten(bytes);

    EXPECT_EQ(bytes, (std::vector<uint8_t> {'a', 'b'}));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
