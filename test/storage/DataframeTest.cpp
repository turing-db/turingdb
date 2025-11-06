#include <gtest/gtest.h>

#include "columns/ColumnIDs.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "dataframe/ColumnTagManager.h"

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
    DataframeManager dfMan;
    Dataframe df;
    ColumnTagManager tagManager;

    ColumnNodeIDs colNodes1;

    const ColumnHeader aHeader(tagManager.allocTag());
    NamedColumn* col1 = NamedColumn::create(&dfMan, &colNodes1, aHeader);
    df.addColumn(col1);
    ASSERT_TRUE(col1 != nullptr);

    ASSERT_EQ(df.cols().size(), 1);

    for (const NamedColumn* namedCol : df.cols()) {
        ASSERT_EQ(namedCol->getHeader().getTag(), aHeader.getTag());
    }
}

TEST_F(DataframeTest, testSameTag) {
    DataframeManager dfMan;
    Dataframe df;
    ColumnTagManager tagManager;

    ColumnNodeIDs colNodes1;
    ColumnNodeIDs colNodes2;
    ColumnEdgeIDs colEdges1;

    const ColumnHeader aHeader(tagManager.allocTag());
    NamedColumn* col1 = NamedColumn::create(&dfMan, &colNodes1, aHeader);
    df.addColumn(col1);
    ASSERT_TRUE(col1 != nullptr);

    const ColumnHeader bHeader(tagManager.allocTag());
    NamedColumn* col2 = NamedColumn::create(&dfMan, &colNodes2, bHeader);
    df.addColumn(col2);
    ASSERT_TRUE(col2 != nullptr);

    EXPECT_THROW({
        auto col = NamedColumn::create(&dfMan, &colEdges1, aHeader);
        df.addColumn(col);
    }, TuringException);

    ASSERT_EQ(df.cols().size(), 2);
}

TEST_F(DataframeTest, anonymous) {
    DataframeManager dfMan;
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

    NamedColumn* col0 = NamedColumn::create(&dfMan, &nodes0, v0Header);
    df.addColumn(col0);
    ASSERT_TRUE(col0 != nullptr);
    ASSERT_EQ(col0->getColumn(), &nodes0);

    NamedColumn* col1 = NamedColumn::create(&dfMan, &nodes1, v1Header);
    df.addColumn(col1);
    ASSERT_TRUE(col1 != nullptr);
    ASSERT_EQ(col1->getColumn(), &nodes1);

    ASSERT_EQ(df.getColumn(v0Header.getTag()), col0);

    // Try to add a column with same anonymous tag
    EXPECT_THROW({
        auto col = NamedColumn::create(&dfMan, &nodes2, v0Header);
        df.addColumn(col);
    }, TuringException);

    // Compare columns of dataframe
    ASSERT_EQ(df.cols().size(), 2);
    std::vector<NamedColumn*> goldVec = {col0, col1};
    ASSERT_EQ(df.cols(), goldVec);
}

TEST_F(DataframeTest, testColNames) {
    DataframeManager dfMan;
    Dataframe df;
    ColumnTagManager tagManager;

    ColumnNodeIDs col0;
    ColumnNodeIDs col1;
    ColumnNodeIDs col2;

    auto colA = NamedColumn::create(&dfMan, &col0, ColumnHeader(tagManager.allocTag()));
    auto colB = NamedColumn::create(&dfMan, &col1, ColumnHeader(tagManager.allocTag()));
    auto colC = NamedColumn::create(&dfMan, &col2, ColumnHeader(tagManager.allocTag()));
    df.addColumn(colA);
    df.addColumn(colB);
    df.addColumn(colC);
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

TEST_F(DataframeTest, dataframes2) {
    DataframeManager dfMan;
    ColumnTagManager tagManager;
    Dataframe df1;
    Dataframe df2;

    ColumnNodeIDs colNodes1;
    ColumnNodeIDs colNodes2;

    NamedColumn* col1 = NamedColumn::create(&dfMan, &colNodes1, ColumnHeader(tagManager.allocTag()));
    df1.addColumn(col1);
    NamedColumn* col2 = NamedColumn::create(&dfMan, &colNodes2, ColumnHeader(tagManager.allocTag()));
    df2.addColumn(col2);

    df2.addColumn(col1);

    ASSERT_EQ(df1.cols().size(), 1);
    ASSERT_EQ(df2.cols().size(), 2);
    ASSERT_EQ(df1.cols(), std::vector<NamedColumn*>({col1}));
    ASSERT_EQ(df2.cols(), std::vector<NamedColumn*>({col2, col1}));
}
