#include "DBComponentTypeRange.h"
#include "ComponentType.h"
#include "DBAccessor.h"
#include "Writeback.h"

#include "gtest/gtest.h"

namespace db {

class DBComponentTypeRangeTest : public ::testing::Test {
protected:
    void SetUp() override {
        _db = DB::create();
        Writeback wb(_db);

        for (size_t i = 0; i < 10; i++) {
            wb.createComponentType(
                _db->getString(std::to_string(i) + "_ComponentType"));
        }
    }

    void TearDown() override {
        delete _db;
    }

    db::DB* _db{nullptr};
};

TEST_F(DBComponentTypeRangeTest, ComponentTypeIteration) {
    const DBAccessor accessor{_db};

    size_t i = 0;
    for (ComponentType* componentType : accessor.componentTypes()) {
        ASSERT_EQ(componentType->getName().getSharedString()->getString(),
                  std::to_string(i) + "_ComponentType");
        i++;
    }
}

} // namespace db
