#include "BioLog.h"
#include "DB.h"
#include "DBDumper.h"
#include "DBLoader.h"
#include "FileUtils.h"
#include "JsonExamples.h"
#include "NodeType.h"
#include "PropertyType.h"
#include "Writeback.h"
#include "gtest/gtest.h"

namespace db {

class LargeDBTest : public ::testing::Test {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        Log::BioLog::init();

        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";
        _logPath = FileUtils::Path(_outDir) / "log";

        // Remove the directory from the previous run
        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }
        FileUtils::createDirectory(_outDir);

        Log::BioLog::init();
        Log::BioLog::openFile(_logPath.string());

        _db = DB::create();
        DBLoader loader {_db, "/home/dev/reactome.out"};
        loader.load();
    }

    void TearDown() override {
        if (_db) {
            delete _db;
        }

        Log::BioLog::destroy();
    }

    DB* _db {nullptr};
    std::string _outDir;
    FileUtils::Path _logPath;
};

TEST_F(LargeDBTest, LoadDB) {
    DBDumper dumper(_db, _outDir);
    dumper.dump();

    db::DB* db = db::DB::create();
    DBLoader loader(db, _outDir);

    ASSERT_TRUE(loader.load());
    ASSERT_TRUE(DBComparator::same(_db, db));

    delete db;
}

}
