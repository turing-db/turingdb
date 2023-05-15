#include "DBNetworkRange.h"
#include "DBAccessor.h"
#include "Network.h"
#include "Writeback.h"

#include "gtest/gtest.h"

namespace db {

class DBNetworkRangeTest : public ::testing::Test {
protected:
    void SetUp() override {
        _db = DB::create();
        Writeback wb(_db);

        for (size_t i = 0; i < 10; i++) {
            wb.createNetwork(_db->getString(std::to_string(i) + "_Net"));
        }
    }

    void TearDown() override {
        delete _db;
    }

    db::DB* _db{nullptr};
};

TEST_F(DBNetworkRangeTest, NetworkIteration) {
    const DBAccessor accessor{_db};

    size_t i = 0;
    for (Network* net : accessor.networks()) {
        ASSERT_EQ(net->getName().getSharedString()->getString(),
                  std::to_string(i) + "_Net");
        i++;
    }
}

} // namespace db
