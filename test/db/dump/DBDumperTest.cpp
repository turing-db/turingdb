#include "gtest/gtest.h"

#include "DBDumper.h"

#include "BioLog.h"

using namespace db;
using namespace Log;

TEST(DBDumperTest, create1) {
    const testing::TestInfo* const testInfo =
      testing::UnitTest::GetInstance()->current_test_info();
    std::string outDir = testInfo->test_suite_name();
    outDir += "_";
    outDir += testInfo->name();
    outDir += ".out";

    BioLog::init();

    DBDumper dumper(outDir);
    const bool success = dumper.dump();
    ASSERT_TRUE(success);
}
