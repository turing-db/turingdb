#include "StringIndexLoader.h"
#include "FileUtils.h"
#include "StringIndex.h"
#include "StringIndexDumper.h"
#include "gtest/gtest.h"

namespace db {

class StringIndexLoaderTest : public ::testing::Test {

protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _indexPath = testInfo->test_suite_name();
        _indexPath += ".smap";
        _indexPath = FileUtils::abspath(_indexPath).string();

        // Remove the file from the previous run
        if (FileUtils::exists(_indexPath)) {
            FileUtils::removeFile(_indexPath);
        }

        for (size_t i = 0; i < 100; i++) {
            std::string rawString = std::to_string(i) + "_str";
            auto s = _index.getString(rawString).getSharedString();
            _strMap[rawString] = s->getID();
        }

        StringIndexDumper dumper(_indexPath);
        dumper.dump(_index);
    }

    void TearDown() override {}

    std::string _indexPath;
    StringIndex _index;
    std::unordered_map<std::string, size_t> _strMap;
};

TEST_F(StringIndexLoaderTest, Load) {
    StringIndexLoader loader(_indexPath);
    StringIndex index;
    std::vector<StringRef> stringRefs;
    loader.load(index);

    for (size_t i = 0; i < 100; i++) {
        std::string s = std::to_string(i) + "_str";

        // Check if the string exists in container in memory
        ASSERT_TRUE(_index.hasString(s));

        // Check if string exists in container loaded from disk
        ASSERT_TRUE(index.hasString(s));


        const SharedString* sstr = index.getString(s).getSharedString();

        // Checking if the ID remains the same
        ASSERT_EQ(_strMap.at(s), sstr->getID());
    }
}

TEST_F(StringIndexLoaderTest, LoadEmpty) {
    StringIndexDumper dumper(_indexPath);
    StringIndexLoader loader(_indexPath);

    StringIndex originalIndex{};
    dumper.dump(originalIndex);

    StringIndex index{};
    std::vector<StringRef> stringRefs;
    loader.load(index);

    ASSERT_EQ(index.getSize(), 0);
}

TEST_F(StringIndexLoaderTest, DumpTwice) {
    StringIndexDumper dumper(_indexPath);
    StringIndexLoader loader(_indexPath);
    std::vector<StringRef> stringRefs;

    // Load original index
    StringIndex index{};
    loader.load(index);

    // Modify the index and dump it
    const SharedString* temp = index.getString("New string").getSharedString();
    const std::string s = temp->getString();
    const SharedString::ID id = temp->getID();
    dumper.dump(index);

    // Reload the index
    StringIndex index2{};
    loader.load(index2);

    // index2 should have one more string
    ASSERT_EQ(index2.getSize(), _index.getSize() + 1);
    // index2 should have the "New string" entry
    ASSERT_TRUE(index2.hasString(s));
    // It should have the same index
    ASSERT_EQ(id, index2.getString(s).getSharedString()->getID());
    // index2 should also have all original strings
    for (size_t i = 0; i < 100; i++) {
        std::string s = std::to_string(i) + "_str";
        ASSERT_TRUE(index2.hasString(s));
        const SharedString* sstr = index2.getString(s).getSharedString();
        ASSERT_EQ(_strMap.at(s), sstr->getID());
    }
}

}
