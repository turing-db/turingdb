#include "DBLoader.h"
#include "DB.h"
#include "DBDumper.h"
#include "FileUtils.h"
#include "JsonExamples.h"
#include "gtest/gtest.h"

namespace db {

class DBLoaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _outDirName = testInfo->test_suite_name();
        _outDirName += "_";
        _outDirName += testInfo->name();
        _outDirName += ".out";
        _outDir = FileUtils::Path(_outDirName);
        _logPath = FileUtils::Path(_outDir) / "log";

        // Remove the directory from the previous run
        if (FileUtils::exists(_outDirName)) {
            FileUtils::removeDirectory(_outDirName);
        }
        FileUtils::createDirectory(_outDirName);
    }

    void TearDown() override {
    }

    void tryDump(DB* db, const FileUtils::Path& outDir) {
        FileUtils::Path dbPath = outDir;
        DBDumper dumper(db, outDir);
        // Checking if dump succesful
        ASSERT_TRUE(dumper.dump());
        ASSERT_TRUE(FileUtils::exists(outDir));
        ASSERT_TRUE(FileUtils::isDirectory(outDir));
        ASSERT_TRUE(FileUtils::exists(dbPath));
        ASSERT_TRUE(FileUtils::isDirectory(dbPath));
    }

    void compareDirectories(const FileUtils::Path& dir1,
                            const FileUtils::Path& dir2) {
        std::vector<FileUtils::Path> paths1;
        std::vector<FileUtils::Path> paths2;
        FileUtils::listFiles(dir1, paths1);
        FileUtils::listFiles(dir2, paths2);

        for (size_t i = 0; i < 3; i++) {
            uint64_t s1 = FileUtils::fileSize(paths1[i]);
            uint64_t s2 = FileUtils::fileSize(paths2[i]);
            ASSERT_EQ(s1, s2);
        }
    }

    void testExampleDB(db::DB* db1) {
        FileUtils::Path dir2 = _outDir / "2";
        FileUtils::Path dir3 = _outDir / "3";
        tryDump(db1, dir2);

        DB* db2 = DB::create();
        DBLoader loader2(db2, dir2);
        ASSERT_TRUE(loader2.load());
        ASSERT_TRUE(DBComparator::same(db1, db2));

        tryDump(db2, dir3);
        DB* db3 = DB::create();
        DBLoader loader3(db3, dir3 );
        ASSERT_TRUE(loader3.load());
        ASSERT_TRUE(DBComparator::same(db1, db3));

        compareDirectories(dir2, dir3);

        delete db1;
        delete db2;
        delete db3;
    }

    std::string _outDirName;
    FileUtils::Path _outDir;
    FileUtils::Path _logPath;
};

TEST_F(DBLoaderTest, EmptyDB) {
    testExampleDB(db::DB::create());
}

TEST_F(DBLoaderTest, CyberSecurityDB) {
    testExampleDB(cyberSecurityDB());
}

//TEST_F(DBLoaderTest, NetworkDB) {
//    testExampleDB(networkDB());
//}
//
//TEST_F(DBLoaderTest, PoleDB) {
//    testExampleDB(poleDB());
//}

TEST_F(DBLoaderTest, StackoverflowDB) {
    testExampleDB(stackoverflowDB());
}

}

