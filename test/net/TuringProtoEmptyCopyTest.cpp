#include <gtest/gtest.h>

#include "TuringProtoInBuf.h"
#include "TuringProtoOutBuf.h"

using namespace net::proto;

TEST(TuringProtoEmptyCopyTest, outBufCopiesNoBytesFromANullPointer) {
    TuringProtoOutBuf outBuf(64);
    outBuf.copyFixedLenData(nullptr, 0);

    EXPECT_EQ(outBuf.size(), 0u);
}

TEST(TuringProtoEmptyCopyTest, inBufReadsNoBytesIntoANullPointer) {
    TuringProtoInBuf inBuf(0);
    inBuf.readData(nullptr, 0);

    EXPECT_EQ(inBuf.readOffset(), 0u);
}
