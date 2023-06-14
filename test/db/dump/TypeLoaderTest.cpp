#include "BioLog.h"
#include "FileUtils.h"
#include "gtest/gtest.h"

namespace db {

class TypeLoaderTest : public ::testing::Test {

protected:
    void SetUp() override {}

    void TearDown() override { Log::BioLog::destroy(); }

    std::string _indexPath;
    std::unordered_map<std::string, size_t> _strMap;
};

TEST_F(TypeLoaderTest, Load) {
}

TEST_F(TypeLoaderTest, LoadEmpty) {
}

TEST_F(TypeLoaderTest, DumpTwice) {
}

}
