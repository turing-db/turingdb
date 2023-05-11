#include "DBDumper.h"
#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "Writeback.h"
#include "gtest/gtest.h"

namespace db {

class DBDumperTest : public ::testing::Test {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        Log::BioLog::init();

        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";
    }

    void TearDown() override {
        if (_db) {
            delete _db;
        }

        Log::BioLog::destroy();
    }

    void tryDump() {
        DBDumper dumper(&_db, _outDir);
        // Checking if dump succesful
        ASSERT_TRUE(dumper.dump());

        // Checking if DBDumperTest_DumpDB.out was created
        ASSERT_TRUE(FileUtils::exists(_outDir));
        ASSERT_TRUE(FileUtils::isDirectory(_outDir));

        // Checking if DBDumperTest_DumpDB.out/turing.out/ was created
        ASSERT_TRUE(FileUtils::exists(dumper.getDBPath()));
        ASSERT_TRUE(FileUtils::isDirectory(dumper.getDBPath()));

        // Checking if DBDumperTest_DumpDB.out/turing.out/smap was created
        ASSERT_TRUE(FileUtils::exists(dumper.getStringIndexPath()));
        ASSERT_FALSE(FileUtils::isDirectory(dumper.getStringIndexPath()));
    }

    std::string _outDir{};
    DB* _db{nullptr};
};

TEST_F(DBDumperTest, DumpDB) {
    _db = DB::create();
    Writeback wb(_db);

    for (size_t i = 0; i < 10; i++) {
        wb.createComponentType(
            _db->getString(std::to_string(i) + "_ComponentType"));
    }

    tryDump();
}

TEST_F(DBDumperTest, DumpEmptyDB) {
    _db = DB::create();
    tryDump();
}

TEST_F(DBDumperTest, DumpDBTwice) {
    _db = DB::create();
    tryDump();
    tryDump();
}

TEST_F(DBDumperTest, DumpLargeDB) {
    _db = DB::create();
    Writeback wb(_db);

    for (size_t i = 0; i < 100000; i++) {
        wb.createComponentType(
            _db->getString(std::to_string(i) + "_ComponentType"));
    }

    tryDump();
}

}
