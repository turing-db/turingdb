#include <gtest/gtest.h>

#include <stdint.h>

#include <limits>
#include <optional>
#include <vector>

#include "columns/BinaryOperators.h"
#include "list/ListBuffer.h"
#include "list/ListElementView.h"
#include "list/ListView.h"

#include "metadata/PropertyNull.h"

using namespace db;

namespace {

class ListIndexOperatorTest : public testing::Test {
protected:
    ListView list(std::vector<QueryListBuffer::ListItemVariant> elements) {
        return _buffer.insert(elements);
    }

    ListElementView cell(QueryListBuffer::ListItemVariant element) {
        return list({element}).elements()[0];
    }

    std::optional<types::Int64::Primitive> integerAt(const std::optional<ListElementView>& element) {
        if (!element.has_value() || element->getTag() != ListBufferTypeTag::Int) {
            return std::nullopt;
        }

        return element->getAs<types::Int64::Primitive>();
    }

    QueryListBuffer _buffer;
};

}

TEST_F(ListIndexOperatorTest, readsThePositionCountingFromTheFront) {
    const ListView elements = list({int64_t {10}, int64_t {20}, int64_t {30}});

    EXPECT_EQ(integerAt(ListIndex {}(elements, int64_t {0})), 10);
    EXPECT_EQ(integerAt(ListIndex {}(elements, int64_t {2})), 30);
}

TEST_F(ListIndexOperatorTest, readsANegativePositionCountingFromTheEnd) {
    const ListView elements = list({int64_t {10}, int64_t {20}, int64_t {30}});

    EXPECT_EQ(integerAt(ListIndex {}(elements, int64_t {-1})), 30);
    EXPECT_EQ(integerAt(ListIndex {}(elements, int64_t {-3})), 10);
}

TEST_F(ListIndexOperatorTest, readsNothingOutsideTheList) {
    const ListView elements = list({int64_t {10}, int64_t {20}, int64_t {30}});

    EXPECT_FALSE(ListIndex {}(elements, int64_t {3}).has_value());
    EXPECT_FALSE(ListIndex {}(elements, int64_t {-4}).has_value());
}

TEST_F(ListIndexOperatorTest, readsAnUnsignedPositionCountingFromTheFront) {
    const ListView elements = list({int64_t {10}, int64_t {20}, int64_t {30}});

    EXPECT_EQ(integerAt(ListIndex {}(elements, uint64_t {0})), 10);
    EXPECT_EQ(integerAt(ListIndex {}(elements, uint64_t {2})), 30);
    EXPECT_FALSE(ListIndex {}(elements, uint64_t {3}).has_value());
}

// An unsigned position is never one counted from the end, so a value that would narrow to
// a negative int64 is out of range rather than a read of the list's tail.
TEST_F(ListIndexOperatorTest, readsNothingAtAnUnsignedPositionAboveTheSignedMaximum) {
    const ListView elements = list({int64_t {10}, int64_t {20}, int64_t {30}});

    const uint64_t justAboveSigned = static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1;

    EXPECT_FALSE(ListIndex {}(elements, std::numeric_limits<uint64_t>::max()).has_value());
    EXPECT_FALSE(ListIndex {}(elements, justAboveSigned).has_value());
}

TEST_F(ListIndexOperatorTest, readsNothingAtANullPosition) {
    const ListView elements = list({int64_t {10}, int64_t {20}});

    EXPECT_FALSE(ListIndex {}(elements, PropertyNull {}).has_value());
}

TEST_F(ListIndexOperatorTest, readsThroughACellHoldingAList) {
    const ListView inner = list({int64_t {10}, int64_t {20}});
    const ListElementView nested = cell(inner);

    EXPECT_EQ(integerAt(ListIndex {}(nested, int64_t {1})), 20);
    EXPECT_FALSE(ListIndex {}(nested, int64_t {2}).has_value());
}

TEST_F(ListIndexOperatorTest, readsNothingThroughACellHoldingAScalar) {
    const ListElementView scalar = cell(int64_t {10});

    EXPECT_FALSE(ListIndex {}(scalar, int64_t {0}).has_value());
}
