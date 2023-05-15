#include "DBLoader.h"
#include "BioLog.h"
#include "DB.h"
#include "DBDumper.h"
#include "FileUtils.h"
#include "Writeback.h"
#include "gtest/gtest.h"

namespace db {

class DBLoaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        Log::BioLog::init();

        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";

        // Remove the directory from the previous run
        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }

        _db = DB::create();
        Writeback wb(_db);

        for (size_t i = 0; i < 10; i++) {
            wb.createComponentType(
                _db->getString(std::to_string(i) + "_ComponentType"));
        }

        DBDumper dumper(_db, _outDir);
        dumper.dump();
    }

    void TearDown() override {
        if (_db) {
            delete _db;
        }

        Log::BioLog::destroy();
    }

    DB* _db{nullptr};
    std::string _outDir;
};

TEST_F(DBLoaderTest, LoadDB) {
    DB* db = DB::create();
    DBLoader loader(db, _outDir);
    ASSERT_TRUE(loader.load());

    for (size_t i = 0; i < 10; i++) {
        std::string s = std::to_string(i) + "_ComponentType";
        const SharedString* loadedString = db->getString(s).getSharedString();
        const SharedString* dumpedString = _db->getString(s).getSharedString();
        ASSERT_EQ(loadedString->getID(), dumpedString->getID());
    }

    delete db;
}

}
