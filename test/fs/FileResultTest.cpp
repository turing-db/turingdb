#include "TuringTest.h"

#include "FileWriter.h"
#include "FileReader.h"
#include "FilePageReader.h"
#include "FilePageWriter.h"
#include "File.h"

class FileResultTest : public TuringTest {
protected:
    void initialize() override {
    }

    void terminate() override {
    }
};

TEST_F(FileResultTest, NotExists) {
    fs::Path p {"/path/to/non/existing"};
    auto fmtMessage = fmt::format("File '/path/to/non/existing' error: "
                                  "Does not exist (No such file or directory)");

    {
        auto res = p.getFileInfo();
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.getPath().c_str(), p.get().c_str());
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ(e.getType(), fs::ErrorType::NOT_EXISTS);
        ASSERT_EQ(e.getErrno(), ENOENT);
    }

    {
        auto res = p.listDir();
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.getPath().c_str(), p.get().c_str());
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ(e.getType(), fs::ErrorType::NOT_EXISTS);
        ASSERT_EQ(e.getErrno(), ENOENT);
    }
}

TEST_F(FileResultTest, OpenFile) {
    fs::Path p {"/path/to/non/existing"};
    auto fmtMessage = fmt::format("File '/path/to/non/existing' error: "
                                  "Could not open file (No such file or directory)");

    {
        auto res = fs::File::open(p);
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.getPath().c_str(), p.get().c_str());
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ(e.getType(), fs::ErrorType::OPEN_FILE);
        ASSERT_EQ(e.getErrno(), ENOENT);
    }
}

TEST_F(FileResultTest, NotDirectory) {
    fs::Path p {"/dev/null"};
    auto fmtMessage = fmt::format("File '/dev/null' error: "
                                  "Not a directory");

    {
        auto res = p.listDir();
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.getPath().c_str(), p.get().c_str());
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ(e.getType(), fs::ErrorType::NOT_DIRECTORY);
        ASSERT_EQ(e.getErrno(), -1);
    }
}

TEST_F(FileResultTest, AlreadyExists) {
    fs::Path p {_outDir};
    auto fmtMessage = fmt::format("File '{}' error: "
                                  "Already exists",
                                  _outDir);

    {
        auto res = p.mkdir();
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.getPath().c_str(), p.get().c_str());
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ((int)e.getType(), (int)fs::ErrorType::ALREADY_EXISTS);
        ASSERT_EQ(e.getErrno(), -1);
    }
}


TEST_F(FileResultTest, CannotMkdir) {
    fs::Path p {"/path/to/non/existing"};
    auto fmtMessage = fmt::format("File '/path/to/non/existing' error: "
                                  "Could not make directory (No such file or directory)");

    {
        auto res = p.mkdir();
        ASSERT_FALSE(res.has_value());
        const auto& e = res.error();
        ASSERT_STREQ(e.getPath().c_str(), p.get().c_str());
        ASSERT_STREQ(e.fmtMessage().c_str(), fmtMessage.c_str());
        ASSERT_EQ((int)e.getType(), (int)fs::ErrorType::CANNOT_MKDIR);
        ASSERT_EQ(e.getErrno(), ENOENT);
    }
}
