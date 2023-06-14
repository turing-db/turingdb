#include "BioLog.h"
#include "FileUtils.h"
#include "gtest/gtest.h"

namespace db {

class TypeDumperTest : public ::testing::Test {

protected:
    void SetUp() override {}

    void TearDown() override { Log::BioLog::destroy(); }

    std::string _indexPath;
    std::unordered_map<std::string, size_t> _strMap;
};

TEST_F(TypeDumperTest, Load) {
}

TEST_F(TypeDumperTest, LoadEmpty) {
}

TEST_F(TypeDumperTest, DumpTwice) {
}

}
