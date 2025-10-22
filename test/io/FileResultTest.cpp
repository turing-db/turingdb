#include "TuringTest.h"

#include "FileReader.h"
#include "File.h"

using namespace turing::test;

class FileResultTest : public TuringTest {
protected:
    void initialize() override {
    }

    void terminate() override {
    }
};

TEST_F(FileResultTest, NotExists) {
    fs::Path p {"/path/to/non/existing"};
    auto fmtMessage = fmt::format("Filesystem error: "
                                  "Does not exist (No such file or directory)");

    {
        auto res = p.getFileInfo();
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ(e.getType(), fs::ErrorType::NOT_EXISTS);
        ASSERT_EQ(e.getErrno(), ENOENT);
    }

    {
        auto res = p.listDir();
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ(e.getType(), fs::ErrorType::NOT_EXISTS);
        ASSERT_EQ(e.getErrno(), ENOENT);
    }
}

TEST_F(FileResultTest, OpenFile) {
    fs::Path p {"/path/to/non/existing"};
    auto fmtMessage = fmt::format("Filesystem error: "
                                  "Could not open file (No such file or directory)");

    {
        auto res = fs::File::open(p);
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ(e.getType(), fs::ErrorType::OPEN_FILE);
        ASSERT_EQ(e.getErrno(), ENOENT);
    }
}

TEST_F(FileResultTest, NotDirectory) {
    fs::Path p {"/dev/null"};
    auto fmtMessage = fmt::format("Filesystem error: "
                                  "Not a directory");

    {
        auto res = p.listDir();
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ(e.getType(), fs::ErrorType::NOT_DIRECTORY);
        ASSERT_EQ(e.getErrno(), -1);
    }
}

TEST_F(FileResultTest, AlreadyExists) {
    fs::Path p {_outDir};
    auto fmtMessage = fmt::format("Filesystem error: "
                                  "Already exists",
                                  _outDir);

    {
        auto res = p.mkdir();
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ((int)e.getType(), (int)fs::ErrorType::ALREADY_EXISTS);
        ASSERT_EQ(e.getErrno(), -1);
    }
}

TEST_F(FileResultTest, CannotMkdir) {
    fs::Path p {"/path/to/non/existing"};
    auto fmtMessage = fmt::format("Filesystem error: "
                                  "Could not make directory (Permission denied)");

    {
        auto res = p.mkdir();
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ((int)e.getType(), (int)fs::ErrorType::CANNOT_MKDIR);
        ASSERT_EQ(e.getErrno(), EACCES);
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 4;
    });
}
