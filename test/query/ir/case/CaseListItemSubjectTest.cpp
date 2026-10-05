#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CaseListItemSubjectTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(CaseListItemSubjectTest, comparesAnUnwoundElement) {
    expectRows("UNWIND [1, 'a'] AS x RETURN CASE x WHEN 1 THEN 'one' ELSE 'other' END",
               {{"one"}, {"other"}});
}

TEST_F(CaseListItemSubjectTest, comparesAnIndexedElement) {
    expectRows("WITH [1, 'a'] AS xs RETURN CASE xs[0] WHEN 1 THEN 'one' ELSE 'other' END",
               {{"one"}});
}

TEST_F(CaseListItemSubjectTest, comparesAgainstAnUnwoundElement) {
    expectRows("UNWIND [1, 'a'] AS x RETURN CASE 'a' WHEN x THEN 'a' ELSE 'other' END",
               {{"a"}, {"other"}});
}

TEST_F(CaseListItemSubjectTest, testsAnUnwoundNull) {
    expectRows("UNWIND [1, null, 'a'] AS x RETURN CASE x WHEN IS NULL THEN 'null' ELSE 'value' END",
               {{"null"}, {"value"}, {"value"}});
}

TEST_F(CaseListItemSubjectTest, ordersAnUnwoundElement) {
    expectRows("UNWIND [1, 5, 'a'] AS x RETURN CASE x WHEN < 3 THEN 'low' ELSE 'other' END",
               {{"low"}, {"other"}, {"other"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
