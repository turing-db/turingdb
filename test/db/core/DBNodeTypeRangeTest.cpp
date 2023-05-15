#include "DBNodeTypeRange.h"
#include "DBAccessor.h"
#include "NodeType.h"
#include "Writeback.h"

#include "gtest/gtest.h"

namespace db {

class DBNodeTypeRangeTest : public ::testing::Test {
protected:
    void SetUp() override {
        _db = DB::create();
        Writeback wb(_db);

        for (size_t i = 0; i < 10; i++) {
            wb.createNodeType(_db->getString(std::to_string(i) + "_NodeType"));
        }
    }

    void TearDown() override {
        delete _db;
    }

    db::DB* _db{nullptr};
};

TEST_F(DBNodeTypeRangeTest, NodeTypeIteration) {
    const DBAccessor accessor{_db};

    size_t i = 0;
    for (NodeType* nodeType : accessor.nodeTypes()) {
        ASSERT_EQ(nodeType->getName().getSharedString()->getString(),
                  std::to_string(i) + "_NodeType");
        i++;
    }
}

} // namespace db
