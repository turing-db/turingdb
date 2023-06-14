#include "TypeDumper.h"
#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "StringIndex.h"
#include "StringIndexDumper.h"
#include "Writeback.h"
#include "gtest/gtest.h"

namespace db {

class TypeIndexDumperTest : public ::testing::Test {

protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _db = DB::create();
        Writeback wb {_db};

        Log::BioLog::init();

        _strIndexPath = testInfo->test_suite_name();
        _strIndexPath += ".smap";
        _strIndexPath = FileUtils::abspath(_strIndexPath).string();

        _indexPath = testInfo->test_suite_name();
        _indexPath += ".types";
        _indexPath = FileUtils::abspath(_indexPath).string();

        for (size_t i = 0; i < 100; i++) {
            const std::string ntRawName = "Node_" + std::to_string(i);
            const StringRef ntName = _strIndex.getString(ntRawName);

            const std::string ptRawNameString =
                "PropertyType_String_" + std::to_string(i);
            const StringRef ptNameString = _strIndex.getString(ptRawNameString);

            const std::string ptRawNameInt =
                "PropertyType_Int_" + std::to_string(i);
            const StringRef ptNameInt = _strIndex.getString(ptRawNameInt);

            NodeType* nt = wb.createNodeType(ntName);
            wb.addPropertyType(nt, ptNameString, {db::ValueType::VK_STRING});
            wb.addPropertyType(nt, ptNameInt, {db::ValueType::VK_INT});
        }
    }

    void TearDown() override { delete _db; Log::BioLog::destroy(); }

    std::string _indexPath;
    std::string _strIndexPath;
    StringIndex _strIndex;
    db::DB* _db {nullptr};
};

TEST_F(TypeIndexDumperTest, DumpStringIndex) {
    if (FileUtils::exists(_strIndexPath)) {
        FileUtils::removeFile(_strIndexPath);
    }

    if (FileUtils::exists(_indexPath)) {
        FileUtils::removeFile(_indexPath);
    }

    StringIndexDumper strDumper(_strIndexPath);
    TypeDumper typeDumper(_db, _indexPath);

    ASSERT_TRUE(typeDumper.dump());
    ASSERT_TRUE(FileUtils::exists(_indexPath));
}

}
