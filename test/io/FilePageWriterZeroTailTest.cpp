#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <fstream>
#include <iterator>
#include <span>
#include <vector>

#include "TuringTest.h"

#include "FilePageWriter.h"
#include "Path.h"

using namespace turing::test;

namespace {

constexpr size_t PAGE_SIZE = 16384;
constexpr uint64_t VALUE = 42;

}

class FilePageWriterZeroTailTest : public TuringTest {
protected:
    void initialize() override {
        _path = fs::Path {_outDir + "/pages"};
    }

    void writeStalePageThenValue(fs::FilePageWriter& writer) {
        const std::vector<uint8_t> stalePage(PAGE_SIZE, 0xFF);
        writer.writeToCurrentPage(std::span<const uint8_t> {stalePage});
        writer.nextPage();

        writer.writeToCurrentPage(VALUE);
    }

    void expectZerosAfterValue() {
        std::ifstream file(_path.c_str(), std::ios::binary);
        const std::vector<uint8_t> bytes {std::istreambuf_iterator<char> {file},
                                          std::istreambuf_iterator<char> {}};
        ASSERT_EQ(bytes.size(), 2 * PAGE_SIZE);

        const uint8_t* secondPage = bytes.data() + PAGE_SIZE;
        uint64_t value {0};
        memcpy(&value, secondPage, sizeof(uint64_t));
        EXPECT_EQ(value, VALUE);

        const bool tailIsZero = std::all_of(secondPage + sizeof(uint64_t),
                                            bytes.data() + bytes.size(),
                                            [](uint8_t byte) { return byte == 0; });
        EXPECT_TRUE(tailIsZero);
    }

    fs::Path _path;
};

TEST_F(FilePageWriterZeroTailTest, SeekWritesZerosAfterTheBytesOfItsPage) {
    {
        auto writer = fs::FilePageWriter::open(_path, PAGE_SIZE);
        ASSERT_TRUE(writer.has_value());

        writeStalePageThenValue(writer.value());
        writer->seek(0);
        ASSERT_FALSE(writer->errorOccured());
    }

    expectZerosAfterValue();
}

TEST_F(FilePageWriterZeroTailTest, NextPageWritesZerosAfterTheBytesOfItsPage) {
    {
        auto writer = fs::FilePageWriter::open(_path, PAGE_SIZE);
        ASSERT_TRUE(writer.has_value());

        writeStalePageThenValue(writer.value());
        writer->nextPage();
        ASSERT_FALSE(writer->errorOccured());
    }

    expectZerosAfterValue();
}

TEST_F(FilePageWriterZeroTailTest, FinishWritesZerosAfterTheBytesOfItsPage) {
    {
        auto writer = fs::FilePageWriter::open(_path, PAGE_SIZE);
        ASSERT_TRUE(writer.has_value());

        writeStalePageThenValue(writer.value());
        writer->finish();
        ASSERT_FALSE(writer->errorOccured());
    }

    expectZerosAfterValue();
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
