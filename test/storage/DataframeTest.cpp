#include <gtest/gtest.h>

#include "columns/Dataframe.h"
#include "columns/ColumnIDs.h"
#include "columns/NamedColumn.h"
#include "columns/ColumnNameManager.h"

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
    Dataframe df;
    ColumnNameManager nameMan;

    ColumnNodeIDs colNodes1;

    const ColumnName aName = nameMan.getName("a");
    NamedColumn* col1 = NamedColumn::create(&df, &colNodes1, aName);
    ASSERT_TRUE(col1 != nullptr);

    ASSERT_EQ(df.cols().size(), 1);

    for (const NamedColumn* namedCol : df.cols()) {
        ASSERT_EQ(namedCol->getPrimaryName(), aName);

        // Retrieve name by doing another lookup into name manager
        ASSERT_EQ(namedCol->getPrimaryName(), nameMan.getName("a"));
    }
}

TEST_F(DataframeTest, testSameName) {
    Dataframe df;
    ColumnNameManager nameMan;

    ColumnNodeIDs colNodes1;
    ColumnNodeIDs colNodes2;
    ColumnEdgeIDs colEdges1;

    NamedColumn* col1 = NamedColumn::create(&df, &colNodes1, nameMan.getName("a"));
    ASSERT_TRUE(col1 != nullptr);

    NamedColumn* col2 = NamedColumn::create(&df, &colNodes2, nameMan.getName("b"));
    ASSERT_TRUE(col2 != nullptr);

    EXPECT_THROW(NamedColumn::create(&df, &colEdges1, nameMan.getName("a")), TuringException);

    ASSERT_EQ(df.cols().size(), 2);
}

TEST_F(DataframeTest, nameManagerNotAnonymous) {
    ColumnNameManager nameMan;

    // Check the string stored for non-anonymous case
    const auto nameToto = nameMan.getName("toto");
    const auto nameTiti = nameMan.getName("titi");
    const auto nameTata = nameMan.getName("tata");
    ASSERT_EQ(nameToto.toStdString(), "toto");
    ASSERT_EQ(nameTiti.toStdString(), "titi");
    ASSERT_EQ(nameTata.toStdString(), "tata");

    // Compare with retrieved name
    ASSERT_EQ(nameToto, nameMan.getName("toto"));
    ASSERT_EQ(nameTiti, nameMan.getName("titi"));
    ASSERT_EQ(nameTata, nameMan.getName("tata"));
    ASSERT_NE(nameToto, nameTiti);
    ASSERT_NE(nameToto, nameTata);
    ASSERT_NE(nameTiti, nameTata);
}

TEST_F(DataframeTest, anonymous) {
    const auto v0Name = ColumnName((size_t)0);
    const auto v1Name = ColumnName(1);
    const auto v2Name = ColumnName(2);
    const auto v3Name = ColumnName(3);

    // Check the string encoding
    ASSERT_EQ(v0Name.toStdString(), "$0");
    ASSERT_EQ(v1Name.toStdString(), "$1");
    ASSERT_EQ(v2Name.toStdString(), "$2");
    ASSERT_EQ(v3Name.toStdString(), "$3");

    // Check with retrieve
    ASSERT_EQ(v0Name, ColumnName((size_t)0));

    ASSERT_TRUE(v0Name.getString() == nullptr);

    Dataframe df;

    ColumnNodeIDs nodes0;
    ColumnNodeIDs nodes1;
    ColumnNodeIDs nodes2;

    NamedColumn* col0 = NamedColumn::create(&df, &nodes0, v0Name);
    ASSERT_TRUE(col0 != nullptr);

    NamedColumn* col1 = NamedColumn::create(&df, &nodes0, v1Name);
    ASSERT_TRUE(col1 != nullptr);

    // Try to add a column with same anonymous tag
    EXPECT_THROW(NamedColumn::create(&df, &nodes2, ColumnName((size_t)0)), TuringException);

    // Compare columns of dataframe
    ASSERT_EQ(df.cols().size(), 2);
    std::vector<NamedColumn*> goldVec = {col0, col1};
    ASSERT_EQ(df.cols(), goldVec);
}
