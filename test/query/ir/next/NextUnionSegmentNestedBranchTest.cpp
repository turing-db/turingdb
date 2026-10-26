#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A UNION segment after NEXT runs each branch once per row it reads, and so does a branch
// that is a UNION or a WHEN of its own. simpledb has 8 people, of whom Remy, Adam, Maxime
// and Luc are French.
class NextUnionSegmentNestedBranchTest : public WriteQueryTest {
};

TEST_F(NextUnionSegmentNestedBranchTest, aNestedUnionRunsOncePerRowRead) {
    expectRows("MATCH (p:Person) RETURN p.name AS n "
               "NEXT { RETURN n AS x UNION ALL RETURN 'q' AS x } UNION ALL RETURN 'r' AS x "
               "NEXT RETURN x, count(*) AS c",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "1"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Suhas", "1"},
                {"Cyrus", "1"},
                {"Doruk", "1"},
                {"q", "8"},
                {"r", "8"}});
}

TEST_F(NextUnionSegmentNestedBranchTest, aNestedWhenRunsOncePerRowRead) {
    expectRows("MATCH (p:Person) RETURN p.name AS n, p.isFrench AS f "
               "NEXT { WHEN f THEN RETURN 'fr' AS x ELSE RETURN 'other' AS x } UNION ALL RETURN 'r' AS x "
               "NEXT RETURN x, count(*) AS c",
               {{"fr", "4"}, {"other", "4"}, {"r", "8"}});
}
