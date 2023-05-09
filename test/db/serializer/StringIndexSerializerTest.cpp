#include "StringIndexSerializer.h"
#include "BioLog.h"
#include "StringIndex.h"
#include "FileUtils.h"
#include "gtest/gtest.h"

namespace db {

class DBSerializerTest : public ::testing::Test {

protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        Log::BioLog::init();

        _indexPath = testInfo->test_suite_name();
        _indexPath += ".strdb";
        _indexPath = FileUtils::abspath(_indexPath).string();

        for (size_t i = 0; i < 10; i++) {
            auto s =
                _index.getString(std::to_string(i) + "_str").getSharedString();
            _strMap[s->getString()] = s->getID();
        }
    }

    void TearDown() override { Log::BioLog::destroy(); }

    std::string _indexPath{};
    StringIndex _index;
    std::unordered_map<std::string, size_t> _strMap;
};


TEST_F(DBSerializerTest, SerializeStringIndexTest) {
    StringIndexSerializer serializer(_indexPath);

    Log::BioLog::echo("Dumping " + _indexPath);
    const Serializer::Result res1 = serializer.dump(_index);
    ASSERT_EQ(res1, Serializer::Result::SUCCESS);

    Log::BioLog::echo("Loading " + _indexPath);
    const Serializer::Result res2 = serializer.load(_index);

    // Checking if load was succeeded
    ASSERT_EQ(res2, Serializer::Result::SUCCESS);

    for (size_t i = 0; i < 10; i++) {
        std::string s = std::to_string(i) + "_str";
        const SharedString* sstr = _index.getString(s).getSharedString();

        // Checking if the ID remains the same
        ASSERT_EQ(_strMap.at(s), sstr->getID());
    }
}

}
