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

        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";
        _logPath = _outDir / "log";

        // Remove the directory from the previous run
        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }
        FileUtils::createDirectory(_outDir);

        _db = DB::create();
        DBLoader loader {_db, "/home/dev/reactome.out/turing.db"};
        loader.load();
    }

    void TearDown() override {
        if (_db) {
            delete _db;
        }
    }

    DB* _db {nullptr};
    FileUtils::Path _outDir;
    FileUtils::Path _logPath;
};

TEST_F(LargeDBTest, LoadDB) {
    DBDumper dumper(_db, _outDir / "turing.db");
    dumper.dump();

    db::DB* db = db::DB::create();
    DBLoader loader(db, _outDir / "turing.db");

    ASSERT_TRUE(loader.load());
    ASSERT_TRUE(DBComparator::same(_db, db));

    delete db;
}

}
