#include "DBLoader.h"
#include "BioLog.h"
#include "DB.h"
#include "DBDumper.h"
#include "Writeback.h"
#include "gtest/gtest.h"

namespace db {

class DBLoaderTest : public ::testing::Test {
protected:
    void SetUp() override {

        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _outPath = testInfo->test_suite_name();
        _outPath += "_";
        _outPath += testInfo->name();
        _outPath += ".out";

        Log::BioLog::init();

        DB* db = DB::create();
        Writeback wb(db);
        DBDumper dumper(db, _outPath);

        // Storing a bunch a strings in the database and storing them in the map
        // to check if the index is correctly retrieved
        for (size_t i = 0; i < 10; i++) {
            std::string s = std::to_string(i) + "_str";
            _strMap[s] = db->getString(s).getSharedString()->getID();
        }

        // Dumping the database
        dumper.dump();

        delete db;
    }

    void TearDown() override { Log::BioLog::destroy(); }

    std::string _outPath = "";
    std::unordered_map<std::string, size_t> _strMap;
};

TEST_F(DBLoaderTest, LoaderStringIndexTest) {
    DB* db{nullptr};
    DBLoader loader(&db, _outPath);
    const DBLoader::Status result = loader.load();
    ASSERT_EQ(result, DBLoader::Status::SUCCESS);

    for (size_t i = 0; i < 10; i++) {
        std::string s = std::to_string(i) + "_str";
        const SharedString* sstr = db->getString(s).getSharedString();
        ASSERT_EQ(_strMap.at(s), sstr->getID());
    }
    delete db;

}
} // namespace db
