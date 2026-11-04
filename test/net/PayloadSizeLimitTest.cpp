#include <gtest/gtest.h>

#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoWriter.h"

using namespace net::proto;

// No packet payload exceeds MAX_PAYLOAD_SIZE: a client refuses a larger one before it
// allocates for it, and a server cannot be built to send one
TEST(PayloadSizeLimitTest, rejectsAPayloadAboveTheMaximum) {
    TuringProtoInBuf inBuf(64);

    EXPECT_THROW(inBuf.reset(MAX_PAYLOAD_SIZE + 1), TuringException);
    EXPECT_EQ(inBuf.capacity(), 64u);
}

TEST(PayloadSizeLimitTest, growsToAPayloadWithinTheMaximum) {
    TuringProtoInBuf inBuf(64);

    inBuf.reset(4096);
    EXPECT_GE(inBuf.capacity(), 4096u);
}

TEST(PayloadSizeLimitTest, rejectsAServerBufferAboveTheMaximum) {
    EXPECT_THROW(TuringProtoWriter writer(MAX_PAYLOAD_SIZE + 1), TuringException);
}
