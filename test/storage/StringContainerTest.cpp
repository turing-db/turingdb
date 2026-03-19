#include <gtest/gtest.h>

#include "StringContainer.h"
#include "TuringException.h"

using namespace db;

TEST(StringContainerTest, OversizedStringThrows) {
    StringContainer container;
    const std::string oversized(StringBucket::BUCKET_SIZE + 1, 'x');

    try {
        container.alloc(oversized);
        FAIL() << "Expected TuringException for oversized string";
    } catch (const TuringException& e) {
        const std::string msg = e.what();
        ASSERT_TRUE(msg.find("exceeds") != std::string::npos
                    || msg.find("too large") != std::string::npos)
            << "Expected user-friendly message, got internal assertion: " << msg;
    }
}

TEST(StringContainerTest, ExactBucketSizeString) {
    StringContainer container;
    const std::string exact(StringBucket::BUCKET_SIZE, 'x');
    ASSERT_NO_THROW(container.alloc(exact));
    ASSERT_EQ(container.size(), 1);
    ASSERT_EQ(container.getView(0).size(), StringBucket::BUCKET_SIZE);
}

TEST(StringContainerTest, General) {
    StringContainer container;
    container.alloc("Hello world");
    container.alloc("");
    container.alloc("Hello people");
    container.alloc("Hello");
    container.alloc("world");
    container.alloc("Hello");
    container.alloc("people");

    const std::string_view v1 = container.getView(0);
    const std::string_view v2 = container.getView(1);
    const std::string_view v3 = container.getView(2);

    ASSERT_EQ(v1.size(), 11);
    ASSERT_EQ(v2.size(), 0);
    ASSERT_EQ(v3.size(), 12);
    ASSERT_TRUE(v1 == "Hello world");
    ASSERT_TRUE(v2 == "");
    ASSERT_TRUE(v3 == "Hello people");
    ASSERT_TRUE(container.getView(3) == "Hello");
    ASSERT_TRUE(container.getView(4) == "world");
    ASSERT_TRUE(container.getView(5) == "Hello");
    ASSERT_TRUE(container.getView(6) == "people");
    ASSERT_EQ(container.size(), 7);
}
