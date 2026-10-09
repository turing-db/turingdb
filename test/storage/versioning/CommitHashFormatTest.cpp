#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include <spdlog/fmt/fmt.h>

#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

using namespace db;

namespace {

template <int I, int Radix>
void expectRendersAs(TemplateCommitHash<I, Radix> hash, const std::string& expected) {
    std::string appended;
    hash.appendString(appended);
    EXPECT_EQ(appended, expected);

    EXPECT_EQ(fmt::format("{}", hash), expected);
    EXPECT_EQ(std::to_string(hash), expected);

    std::ostringstream stream;
    stream << hash;
    EXPECT_EQ(stream.str(), expected);

    const auto parsed = TemplateCommitHash<I, Radix>::fromString(expected);
    ASSERT_TRUE(parsed);
    EXPECT_EQ(parsed.value(), hash);
}

}

TEST(CommitHashFormatTest, commitHashRendersInHex) {
    expectRendersAs(CommitHash {0xabcdef}, "abcdef");
}

TEST(CommitHashFormatTest, changeIDRendersInDecimal) {
    expectRendersAs(ChangeID {16}, "16");
}

TEST(CommitHashFormatTest, headRendersAsHead) {
    expectRendersAs(CommitHash::head(), "head");
    expectRendersAs(ChangeID::head(), "head");
}
