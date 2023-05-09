#include "DBSerializer.h"
#include "BioLog.h"
#include "DB.h"
#include "Writeback.h"
#include "gtest/gtest.h"

namespace db {

class DBSerializerTest : public ::testing::Test {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        Log::BioLog::init();

        _dbPath = testInfo->test_suite_name();
        _dbPath += "_";
        _dbPath += testInfo->name();
        _dbPath += ".out";

        _db = DB::create();
        Writeback wb(_db);

        for (size_t i = 0; i < 10; i++) {
            wb.createComponentType(
                _db->getString(std::to_string(i) + "_ComponentType"));
        }
    }

    void TearDown() override {
        if (_db) {
            delete _db;
        }
        Log::BioLog::destroy();
    }

    std::string _dbPath{};
    DB* _db{nullptr};
    std::unordered_map<std::string, size_t> _strMap;
};

TEST_F(DBSerializerTest, DumpStringIndexTest) {
    DBSerializer serializer(&_db, _dbPath);
    const Serializer::Result result = serializer.dump();
    ASSERT_EQ(result, Serializer::Result::SUCCESS);
}

TEST_F(DBSerializerTest, LoadStringIndexTest) {
    DB* db{nullptr};
    DBSerializer serializer(&db, _dbPath);
    const Serializer::Result res = serializer.load();
    ASSERT_EQ(res, Serializer::Result::SUCCESS);

    for (size_t i = 0; i < 10; i++) {
        std::string s = std::to_string(i) + "_str";
        const SharedString* sstr = db->getString(s).getSharedString();
        ASSERT_EQ(_strMap.at(s), sstr->getID());
    }
    delete db;
}
} // namespace db
