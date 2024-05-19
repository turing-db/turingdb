#include "CSVImport.h"
#include "DB.h"
#include "FileUtils.h"
#include "Network.h"
#include "Node.h"
#include "StringBuffer.h"
#include "Writeback.h"
#include "gtest/gtest.h"
#include "LogSetup.h"

#include <spdlog/spdlog.h>

namespace db {

class CSVImportTest : public ::testing::Test {
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

        LogSetup::setupLogFileBacked(_logPath.string());
    }

    void TearDown() override {
    }

    std::tuple<db::DB*, bool> run(
        const FileUtils::Path& path,
        std::string primaryColumn = "",
        const char delimiter = ',') {

        db::DB* db = DB::create();
        Writeback wb(db);
        db::Network* net = wb.createNetwork(db->getString("example_net"));
        StringBuffer* strBuffer = StringBuffer::readFromFile(path);
        CSVImport csvImport({
            .buffer = strBuffer,
            .db = db,
            .outNet = net,
            .delimiter = delimiter,
            .primaryColumn = primaryColumn,
        });
        const bool success = csvImport.run();
        delete strBuffer;

        return {db, success};
    }

    std::string _outDirName;
    FileUtils::Path _outDir;
    FileUtils::Path _logPath;
};

TEST_F(CSVImportTest, ImportEmptyLinesEnd) {
    auto [db1, success1] = run("1_empty_line_end.csv");
    ASSERT_EQ(db1->nodes().size(), 8);
    ASSERT_EQ(db1->nodeTypes().size(), 4);
    ASSERT_EQ(db1->edges().size(), 6);
    ASSERT_EQ(db1->edgeTypes().size(), 3);
    ASSERT_TRUE(success1);
    auto [db2, success2] = run("2_empty_line_end.csv");
    ASSERT_TRUE(success2);
    ASSERT_TRUE(DBComparator::same(db1, db2));
    auto [db3, success3] = run("3_empty_line_end.csv");
    ASSERT_TRUE(success3);
    ASSERT_TRUE(DBComparator::same(db1, db3));
}

TEST_F(CSVImportTest, ImportOneColumn) {
    auto [db, success] = run("one_column.csv");
    ASSERT_TRUE(success);
    ASSERT_EQ(db->nodes().size(), 2);
    ASSERT_EQ(db->edges().size(), 0);
    ASSERT_EQ(db->nodeTypes().size(), 1);
    ASSERT_EQ(db->edgeTypes().size(), 0);
}

TEST_F(CSVImportTest, Whitespaces) {
    auto [db, success] = run("whitespaces.csv");
    ASSERT_TRUE(success);
    ASSERT_EQ(db->nodes().size(), 8);
    ASSERT_EQ(db->nodeTypes().size(), 4);
    ASSERT_EQ(db->edges().size(), 6);
    ASSERT_EQ(db->edgeTypes().size(), 3);
}

TEST_F(CSVImportTest, StringsExample) {
    auto [db, success] = run("only_strings.csv");
    ASSERT_TRUE(success);
    ASSERT_EQ(db->nodes().size(), 8);
    ASSERT_EQ(db->nodeTypes().size(), 4);
    ASSERT_EQ(db->edges().size(), 6);
    ASSERT_EQ(db->edgeTypes().size(), 3);
}

TEST_F(CSVImportTest, RealExample) {
    auto [db1, success1] = run("example1.csv");
    ASSERT_TRUE(success1);
    auto [db2, success2] = run("example2.csv");
    ASSERT_TRUE(success2);
}

TEST_F(CSVImportTest, WithFloats) {
    auto [db, success] = run("strings_and_floats.csv");
    ASSERT_TRUE(success);
}

TEST_F(CSVImportTest, Duplicates) {
    auto [db, success] = run("duplicates.csv");
    ASSERT_TRUE(success);
    ASSERT_EQ(db->nodes().size(), 9);
    ASSERT_EQ(db->nodeTypes().size(), 4);
    ASSERT_EQ(db->edges().size(), 9);
    ASSERT_EQ(db->edgeTypes().size(), 3);
}

TEST_F(CSVImportTest, DuplicateNodeType) {
    auto [db, success] = run("duplicates_in_header.csv");
    ASSERT_FALSE(success);

    LogSetup::logFlush();

    std::string logContent;
    ASSERT_TRUE(FileUtils::readContent(_logPath, logContent));
    ASSERT_TRUE(logContent.ends_with("[error] Duplicate in CSV header Diagnostic\n"));
}

TEST_F(CSVImportTest, MissingNames) {
    auto [db, success] = run("missing_cell_value.csv");
    ASSERT_TRUE(success);

    const auto* n0 = db->getNode((DBIndex)0);
    const auto name0 = n0->getName().getSharedString()->getString();
    ASSERT_STREQ(name0.data(), "John");

    const auto* n1 = db->getNode((DBIndex)1);
    const auto name1 = n1->getName().getSharedString()->getString();
    ASSERT_STREQ(name1.data(), "Mount Sinai Hospital");

    ASSERT_EQ(db->nodes().size(), 7);
    ASSERT_EQ(db->nodeTypes().size(), 4);
    ASSERT_EQ(db->edges().size(), 5);
    ASSERT_EQ(db->edgeTypes().size(), 3);
}

TEST_F(CSVImportTest, MissingEntry) {
    auto [db, success] = run("missing_entry.csv");
    ASSERT_FALSE(success);

    LogSetup::logFlush();

    std::string logContent;
    ASSERT_TRUE(FileUtils::readContent(_logPath, logContent));
    ASSERT_TRUE(logContent.ends_with("[error] Missing CSV entry at line 2\n"));
}

TEST_F(CSVImportTest, TooManyEntries) {
    auto [db, success] = run("too_many_entries.csv");
    ASSERT_FALSE(success);

    LogSetup::logFlush();

    std::string logContent;
    ASSERT_TRUE(FileUtils::readContent(_logPath, logContent));
    ASSERT_TRUE(logContent.ends_with("[error] Too many entries at line 2\n"));
}

TEST_F(CSVImportTest, PrimaryColumn) {
    // Test invalid primary column
    auto [db1, success1] = run("only_strings.csv", "InvalidPrimary");
    ASSERT_FALSE(success1);

    LogSetup::logFlush();

    std::string logContent;
    ASSERT_TRUE(FileUtils::readContent(_logPath, logContent));
    ASSERT_TRUE(logContent.ends_with("[error] Specified primary column InvalidPrimary is invalid\n"));

    auto [db2, success2] = run("only_strings.csv", "Patient");
    ASSERT_TRUE(success2);
    ASSERT_EQ(db2->nodes().size(), 8);
    ASSERT_EQ(db2->nodeTypes().size(), 4);
    ASSERT_EQ(db2->edges().size(), 6);
    ASSERT_EQ(db2->edgeTypes().size(), 3);
}

TEST_F(CSVImportTest, Delimiters) {
    // Test ',' and ';' delimiters
    auto [db, success] = run("example_semicolon.csv", "", ';');
    ASSERT_TRUE(success);
}
}
