#include <gtest/gtest.h>

#include "columns/ColumnIDs.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

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

    ColumnNodeIDs colNodes1;

    const auto aTag = dfMan.allocTag();
    NamedColumn* col1 = NamedColumn::create(&dfMan, &colNodes1, aTag);
    df.addColumn(col1);
    ASSERT_TRUE(col1 != nullptr);

    ASSERT_EQ(df.cols().size(), 1);

    for (const NamedColumn* namedCol : df.cols()) {
        ASSERT_EQ(namedCol->getTag(), aTag);
    }
}

TEST_F(DataframeTest, anonymous) {
    DataframeManager dfMan;

    const auto v0Tag = dfMan.allocTag();
    const auto v1Tag = dfMan.allocTag();
    const auto v2Tag = dfMan.allocTag();
    const auto v3Tag = dfMan.allocTag();

    ASSERT_EQ(v0Tag, ColumnTag(0));
    ASSERT_EQ(v1Tag, ColumnTag(1));
    ASSERT_EQ(v2Tag, ColumnTag(2));
    ASSERT_EQ(v3Tag, ColumnTag(3));

    Dataframe df;

    ColumnNodeIDs nodes0;
    ColumnNodeIDs nodes1;

    NamedColumn* col0 = NamedColumn::create(&dfMan, &nodes0, v0Tag);
    df.addColumn(col0);
    ASSERT_TRUE(col0 != nullptr);
    ASSERT_EQ(col0->getColumn(), &nodes0);

    NamedColumn* col1 = NamedColumn::create(&dfMan, &nodes1, v1Tag);
    df.addColumn(col1);
    ASSERT_TRUE(col1 != nullptr);
    ASSERT_EQ(col1->getColumn(), &nodes1);

    ASSERT_EQ(df.getColumn(v0Tag), col0);

    // Compare columns of dataframe
    const std::vector<NamedColumn*> goldVec = {col0, col1};
    ASSERT_EQ(df.cols(), goldVec);
}

TEST_F(DataframeTest, testColNames) {
    DataframeManager dfMan;
    Dataframe df;

    ColumnNodeIDs col0;
    ColumnNodeIDs col1;
    ColumnNodeIDs col2;

    auto colA = NamedColumn::create(&dfMan, &col0, dfMan.allocTag());
    auto colB = NamedColumn::create(&dfMan, &col1, dfMan.allocTag());
    auto colC = NamedColumn::create(&dfMan, &col2, dfMan.allocTag());
    df.addColumn(colA);
    df.addColumn(colB);
    df.addColumn(colC);
    ASSERT_TRUE(colA != nullptr);
    ASSERT_TRUE(colB != nullptr);
    ASSERT_TRUE(colC != nullptr);

    // Change name of middle column
    colB->rename("middle1");
    ASSERT_EQ(colB->getName(), "middle1");

    // Check that the dataframe still has 3 columns
    ASSERT_EQ(df.cols().size(), 3);

    // Check that we retrieve colB using the tag
    ASSERT_EQ(df.getColumn(ColumnTag(1)), colB);
}

TEST_F(DataframeTest, dataframes2) {
    DataframeManager dfMan;
    Dataframe df1;
    Dataframe df2;

    ColumnNodeIDs colNodes1;
    ColumnNodeIDs colNodes2;

    NamedColumn* col1 = NamedColumn::create(&dfMan, &colNodes1, dfMan.allocTag());
    df1.addColumn(col1);
    NamedColumn* col2 = NamedColumn::create(&dfMan, &colNodes2, dfMan.allocTag());
    df2.addColumn(col2);

    df2.addColumn(col1);

    ASSERT_EQ(df1.cols().size(), 1);
    ASSERT_EQ(df2.cols().size(), 2);
    ASSERT_EQ(df1.cols(), std::vector<NamedColumn*>({col1}));
    ASSERT_EQ(df2.cols(), std::vector<NamedColumn*>({col2, col1}));
}

TEST_F(DataframeTest, nonMonoticTags) {
    DataframeManager dfMan;
    Dataframe df;

    NamedColumn* col1 = NamedColumn::create(&dfMan, nullptr, ColumnTag(42));
    df.addColumn(col1);
    NamedColumn* col2 = NamedColumn::create(&dfMan, nullptr, ColumnTag(24));
    df.addColumn(col2);
    NamedColumn* col3 = NamedColumn::create(&dfMan, nullptr, ColumnTag(0));
    df.addColumn(col3);

    ASSERT_EQ(df.cols().size(), 3);
    ASSERT_EQ(df.cols(), std::vector<NamedColumn*>({col1, col2, col3}));

    ASSERT_EQ(df.getColumn(ColumnTag(0)), col3);
    ASSERT_EQ(df.getColumn(ColumnTag(24)), col2);
    ASSERT_EQ(df.getColumn(ColumnTag(42)), col1);
    ASSERT_EQ(df.getColumn(ColumnTag(17)), nullptr);
}
