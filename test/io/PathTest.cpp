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
        fs::Path p("/path/to/non/existing");
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
        fs::Path p(_outDir);
        auto res = p.getFileInfo();
        ASSERT_TRUE(res.has_value());
        const auto& info = res.value();
        ASSERT_EQ(info._type, fs::FileType::Directory);
        ASSERT_TRUE(info.readable());
        ASSERT_TRUE(info.writable());
    }

    { // File
        fs::Path p(_logPath);
        auto res = p.getFileInfo();
        ASSERT_TRUE(res.has_value());
        const auto& info = res.value();
        ASSERT_EQ(info._type, fs::FileType::File);
        ASSERT_TRUE(info.readable());
        ASSERT_TRUE(info.writable());
    }
}

TEST_F(PathTest, Mkdir) {
    fs::Path p(_outDir);
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

// The containment check every command that reads or writes a path the query named
// resolves it through: whatever the path spells, what counts is where it lands.
TEST_F(PathTest, IsSubDirectory) {
    const fs::Path root(_outDir);

    { // A path under the root, however deep
        ASSERT_TRUE((root / "file").isSubDirectory(root));
        ASSERT_TRUE((root / "sub" / "deeper" / "file").isSubDirectory(root));
    }

    { // A climb that lands back inside is still inside
        ASSERT_TRUE((root / "sub" / ".." / "file").isSubDirectory(root));
    }

    { // A climb that leaves is out, whether it is spelled or already resolved
        ASSERT_FALSE((root / "..").isSubDirectory(root));
        ASSERT_FALSE((root / ".." / "sibling").isSubDirectory(root));
        ASSERT_FALSE((root / ".." / ".." / "etc" / "passwd").isSubDirectory(root));
        ASSERT_FALSE(fs::Path("/etc/passwd").isSubDirectory(root));
    }

    { // The root resolves to "." against itself, which counts as inside - so a
      // command handed an empty path resolves to the directory itself and fails on
      // opening it rather than on the containment check
        ASSERT_TRUE(root.isSubDirectory(root));
    }

    { // A sibling whose name starts with the root's is not under it
        ASSERT_FALSE(fs::Path(_outDir + "_other").isSubDirectory(root));
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 4;
    });
}
