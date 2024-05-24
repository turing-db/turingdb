#include <gtest/gtest.h>

#include "DBMetaData.h"
#include "EdgeContainer.h"
#include "EntityID.h"

using namespace db;

class EdgeContainerTest : public ::testing::Test {
protected:
    void SetUp() override {
    }

    void TearDown() override {
    }

    DBMetaData _metaData;
};

TEST_F(EdgeContainerTest, UnsortedNodeIDsFail) {
    std::vector<EdgeRecord> outEdges = {
        {0, 1, 2, 0},
        {1, 0, 3, 0},
    };
    std::vector<EdgeRecord> inEdges = {
        {1, 2, 1, 0},
        {0, 3, 0, 0},
    };
}

TEST_F(EdgeContainerTest, SortedSuccess) {
}

TEST_F(EdgeContainerTest, IterateAll) {
}

TEST_F(EdgeContainerTest, IterateByLabels) {
}

TEST_F(EdgeContainerTest, GetNodeLabelset) {
}
