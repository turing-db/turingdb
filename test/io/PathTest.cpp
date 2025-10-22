#include "TuringTest.h"

#include "Path.h"

using namespace turing::test;

class PathTest : public TuringTest {
protected:
    void initialize() override {
    }

    void terminate() override {
    }
};

TEST_F(PathTest, GetFileInfo) {
    { // Non existing
        fs::Path p {"/path/to/non/existing"};
        auto fmtMessage = fmt::format("Filesystem error: "
                                      "Does not exist (No such file or directory)");

        auto res = p.getFileInfo();
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ(e.getType(), fs::ErrorType::NOT_EXISTS);
        ASSERT_EQ(e.getErrno(), ENOENT);
    }

    { // Directory
        fs::Path p {_outDir};
        auto res = p.getFileInfo();
        ASSERT_TRUE(res.has_value());
        const auto& info = res.value();
        ASSERT_EQ(info._type, fs::FileType::Directory);
        ASSERT_TRUE(info.readable());
        ASSERT_TRUE(info.writable());
    }

    { // File
        fs::Path p {_logPath};
        auto res = p.getFileInfo();
        ASSERT_TRUE(res.has_value());
        const auto& info = res.value();
        ASSERT_EQ(info._type, fs::FileType::File);
        ASSERT_TRUE(info.readable());
        ASSERT_TRUE(info.writable());
    }
}

TEST_F(PathTest, Mkdir) {
    fs::Path p {_outDir};
    p /= "path";
    p /= "to";
    p /= "sub";
    p /= "dir";

    auto res = p.mkdir();
    ASSERT_TRUE(res);

    auto res2 = p.getFileInfo();
    ASSERT_TRUE(res2.has_value());
    const auto& info = res2.value();
    ASSERT_EQ(info._type, fs::FileType::Directory);
    ASSERT_TRUE(info.readable());
    ASSERT_TRUE(info.writable());
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 4;
    });
}
