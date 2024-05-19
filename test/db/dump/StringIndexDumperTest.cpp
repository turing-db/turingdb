#include "StringIndexDumper.h"
#include "FileUtils.h"
#include "StringIndex.h"
#include "gtest/gtest.h"

namespace db {

class StringIndexDumperTest : public ::testing::Test {

protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _indexPath = testInfo->test_suite_name();
        _indexPath += ".smap";
        _indexPath = FileUtils::abspath(_indexPath).string();

        for (size_t i = 0; i < 1000; i++) {
            std::string rawString = std::to_string(i) + "_str";
            _index.getString(rawString).getSharedString();
        }
    }

    void TearDown() override {}

    std::string _indexPath;
    StringIndex _index;
};

TEST_F(StringIndexDumperTest, DumpStringIndex) {
    if (FileUtils::exists(_indexPath)) {
        FileUtils::removeFile(_indexPath);
    }

    StringIndexDumper dumper(_indexPath);

    ASSERT_EQ(dumper.dump(_index), true);
    ASSERT_TRUE(FileUtils::exists(_indexPath));
}

}
