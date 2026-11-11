#include <utility>

#include "TuringTest.h"

#include "AlignedBuffer.h"

using namespace turing::test;

namespace {

constexpr size_t CAPACITY = 16384;

}

class AlignedBufferMoveTest : public TuringTest {
};

TEST_F(AlignedBufferMoveTest, MoveConstructionKeepsTheCapacity) {
    fs::AlignedBuffer buffer(CAPACITY);
    const fs::AlignedBuffer moved(std::move(buffer));

    EXPECT_EQ(moved.capacity(), CAPACITY);
}

TEST_F(AlignedBufferMoveTest, MoveAssignmentKeepsTheCapacity) {
    fs::AlignedBuffer buffer(CAPACITY);
    fs::AlignedBuffer assigned;
    assigned = std::move(buffer);

    EXPECT_EQ(assigned.capacity(), CAPACITY);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
