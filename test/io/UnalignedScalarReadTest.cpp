#include <stdint.h>
#include <string.h>

#include "TuringTest.h"

#include "AlignedBuffer.h"
#include "ByteBuffer.h"
#include "ByteBufferIterator.h"
#include "File.h"

using namespace turing::test;

class UnalignedScalarReadTest : public TuringTest {
protected:
    static constexpr uint32_t HEADER = 7;
    static constexpr uint64_t VALUE = 0x0123456789abcdef;
};

TEST_F(UnalignedScalarReadTest, alignedBufferIteratorReadsAScalarPackedAfterASmallerOne) {
    fs::AlignedBuffer buffer;
    buffer.write(reinterpret_cast<const uint8_t*>(&HEADER), sizeof(HEADER));
    buffer.write(reinterpret_cast<const uint8_t*>(&VALUE), sizeof(VALUE));

    fs::AlignedBufferIterator iterator(buffer);
    iterator.advance<uint32_t>();

    EXPECT_EQ(iterator.get<uint64_t>(), VALUE);
}

TEST_F(UnalignedScalarReadTest, byteBufferIteratorReadsAScalarPackedAfterASmallerOne) {
    fs::ByteBuffer buffer(sizeof(HEADER) + sizeof(VALUE));
    memcpy(buffer.data(), &HEADER, sizeof(HEADER));
    memcpy(buffer.data() + sizeof(HEADER), &VALUE, sizeof(VALUE));

    fs::ByteBufferIterator iterator(buffer);
    iterator.advance<uint32_t>();

    EXPECT_EQ(iterator.get<uint64_t>(), VALUE);
}

TEST_F(UnalignedScalarReadTest, fileRegionReadsAScalarPackedAfterASmallerOne) {
    uint8_t bytes[sizeof(HEADER) + sizeof(VALUE)];
    memcpy(bytes, &HEADER, sizeof(HEADER));
    memcpy(bytes + sizeof(HEADER), &VALUE, sizeof(VALUE));

    const fs::Path path(_outDir + "/packed");
    auto file = fs::File::createAndOpen(path);
    ASSERT_TRUE(file.has_value());
    ASSERT_TRUE(file->write(bytes, sizeof(bytes)));

    auto region = file->map(sizeof(bytes));
    ASSERT_TRUE(region.has_value());

    EXPECT_EQ(region->read<uint64_t>(sizeof(HEADER)), VALUE);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
