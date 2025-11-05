#include <gtest/gtest.h>

#include "columns/ColumnIDs.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "dataframe/ColumnTagManager.h"

#include "LocalMemory.h"

#include "TuringException.h"

using namespace db;

class DataframeTest : public ::testing::Test {
protected:
    void SetUp() override {
    }

    void TearDown() override {
    }
};

TEST_F(DataframeTest, testEmpty) {
    Dataframe df;
    ASSERT_TRUE(df.cols().empty());
}

TEST_F(DataframeTest, testOneCol) {
    LocalMemory mem;
    Dataframe df;
    ColumnTagManager tagManager;

    ColumnNodeIDs colNodes1;

    const ColumnHeader aHeader(tagManager.allocTag());
    NamedColumn* col1 = mem.alloc<NamedColumn>(&df, aHeader, &colNodes1);
    ASSERT_TRUE(col1 != nullptr);

    ASSERT_EQ(df.cols().size(), 1);

    for (const NamedColumn* namedCol : df.cols()) {
        ASSERT_EQ(namedCol->getHeader().getTag(), aHeader.getTag());
    }
}

TEST_F(DataframeTest, testSameTag) {
    LocalMemory mem;
    Dataframe df;
    ColumnTagManager tagManager;

    ColumnNodeIDs colNodes1;
    ColumnNodeIDs colNodes2;
    ColumnEdgeIDs colEdges1;

    const ColumnHeader aHeader(tagManager.allocTag());
    NamedColumn* col1 = mem.alloc<NamedColumn>(&df, aHeader, &colNodes1);
    ASSERT_TRUE(col1 != nullptr);

    const ColumnHeader bHeader(tagManager.allocTag());
    NamedColumn* col2 = mem.alloc<NamedColumn>(&df, bHeader, &colNodes2);
    ASSERT_TRUE(col2 != nullptr);

    EXPECT_THROW(mem.alloc<NamedColumn>(&df, aHeader, &colEdges1), TuringException);

    ASSERT_EQ(df.cols().size(), 2);
}

TEST_F(DataframeTest, anonymous) {
    LocalMemory mem;
    ColumnTagManager tagManager;

    const auto v0Header = ColumnHeader(tagManager.allocTag());
    const auto v1Header = ColumnHeader(tagManager.allocTag());
    const auto v2Header = ColumnHeader(tagManager.allocTag());
    const auto v3Header = ColumnHeader(tagManager.allocTag());

    ASSERT_EQ(v0Header.getTag(), ColumnTag(0));
    ASSERT_EQ(v1Header.getTag(), ColumnTag(1));
    ASSERT_EQ(v2Header.getTag(), ColumnTag(2));
    ASSERT_EQ(v3Header.getTag(), ColumnTag(3));

    Dataframe df;

    ColumnNodeIDs nodes0;
    ColumnNodeIDs nodes1;
    ColumnNodeIDs nodes2;

    NamedColumn* col0 = mem.alloc<NamedColumn>(&df, v0Header, &nodes0);
    ASSERT_TRUE(col0 != nullptr);
    ASSERT_EQ(col0->getColumn(), &nodes0);

    NamedColumn* col1 = mem.alloc<NamedColumn>(&df, v1Header, &nodes1);
    ASSERT_TRUE(col1 != nullptr);
    ASSERT_EQ(col1->getColumn(), &nodes1);

    ASSERT_EQ(df.getColumn(v0Header.getTag()), col0);

    // Try to add a column with same anonymous tag
    EXPECT_THROW(mem.alloc<NamedColumn>(&df, v0Header, &nodes2), TuringException);

    // Compare columns of dataframe
    ASSERT_EQ(df.cols().size(), 2);
    std::vector<NamedColumn*> goldVec = {col0, col1};
    ASSERT_EQ(df.cols(), goldVec);
}

TEST_F(DataframeTest, testColNames) {
    LocalMemory mem;
    Dataframe df;
    ColumnTagManager tagManager;

    ColumnNodeIDs col0;
    ColumnNodeIDs col1;
    ColumnNodeIDs col2;

    auto colA = mem.alloc<NamedColumn>(&df, ColumnHeader(tagManager.allocTag()), &col0);
    auto colB = mem.alloc<NamedColumn>(&df, ColumnHeader(tagManager.allocTag()), &col1);
    auto colC = mem.alloc<NamedColumn>(&df, ColumnHeader(tagManager.allocTag()), &col2);
    ASSERT_TRUE(colA != nullptr);
    ASSERT_TRUE(colB != nullptr);
    ASSERT_TRUE(colC != nullptr);

    // Change name of middle column
    colB->getHeader().setName("middle1");
    ASSERT_EQ(colB->getHeader().getName(), "middle1");

    // Check that the dataframe still has 3 columns
    ASSERT_EQ(df.cols().size(), 3);

    // Check that we retrieve colB using the tag
    ASSERT_EQ(df.getColumn(ColumnTag(1)), colB);
}
