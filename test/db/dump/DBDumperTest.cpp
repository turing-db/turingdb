#include "DBDumper.h"
#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "JsonExamples.h"
#include "Writeback.h"
#include "gtest/gtest.h"

namespace db {

class DBDumperTest : public ::testing::Test {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";
        _logPath = FileUtils::Path(_outDir) / "log";
        _dbPath = FileUtils::abspath(FileUtils::Path(_outDir) / "turing.db");

        // Remove the directory from the previous run
        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }
        FileUtils::createDirectory(_outDir);

        _stringIndexPath = _dbPath / "smap";
        _typeIndexPath = _dbPath / "types";
        _entityIndexPath = _dbPath / "data";

        Log::BioLog::init();
        Log::BioLog::openFile(_logPath.string());
    }

    void TearDown() override {
        if (_db) {
            delete _db;
        }

        Log::BioLog::destroy();
    }

    void tryDump() {
        DBDumper dumper(_db, _dbPath);
        // Checking if dump succesful
        ASSERT_TRUE(dumper.dump());

        // Checking if DBDumperTest_DumpDB.out was created
        ASSERT_TRUE(FileUtils::exists(_outDir));
        ASSERT_TRUE(FileUtils::isDirectory(_outDir));

        // Checking if DBDumperTest_DumpDB.out/turing.db/ was created
        ASSERT_TRUE(FileUtils::exists(_dbPath));
        ASSERT_TRUE(FileUtils::isDirectory(_dbPath));

        // Checking if DBDumperTest_DumpDB.out/turing.db/smap was created
        ASSERT_TRUE(FileUtils::exists(_stringIndexPath));
        ASSERT_FALSE(FileUtils::isDirectory(_stringIndexPath));

        // Checking if DBDumperTest_DumpDB.out/turing.db/types was created
        ASSERT_TRUE(FileUtils::exists(_typeIndexPath));
        ASSERT_FALSE(FileUtils::isDirectory(_typeIndexPath));

        // Checking if DBDumperTest_DumpDB.out/turing.db/entities was created
        ASSERT_TRUE(FileUtils::exists(_entityIndexPath));
        ASSERT_FALSE(FileUtils::isDirectory(_entityIndexPath));
    }

    DB* _db {nullptr};
    std::string _outDir;
    FileUtils::Path _logPath;
    FileUtils::Path _dbPath;
    FileUtils::Path _stringIndexPath;
    FileUtils::Path _typeIndexPath;
    FileUtils::Path _entityIndexPath;
};

TEST_F(DBDumperTest, DumpDB) {
    _db = cyberSecurityDB();
    tryDump();
}

TEST_F(DBDumperTest, DumpEmptyDB) {
    _db = DB::create();
    tryDump();
}

TEST_F(DBDumperTest, DumpDBTwice) {
    _db = cyberSecurityDB();
    tryDump();
    tryDump();
}

}
