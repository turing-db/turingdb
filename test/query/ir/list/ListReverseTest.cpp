#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class ListReverseTest : public WriteQueryTest {
};

TEST_F(ListReverseTest, reversesALiteralList) {
    expectRows("RETURN reverse([1, 2, 3]), reverse([]), reverse([1])", {{"[3, 2, 1]", "[]", "[1]"}});
}

TEST_F(ListReverseTest, reversesAHeterogeneousList) {
    expectRows("RETURN reverse([1, 'two', true, null])", {{"[null, true, two, 1]"}});
}

TEST_F(ListReverseTest, keepsNestedListsWhole) {
    expectRows("RETURN reverse([[1, 2], [3]])", {{"[[3], [1, 2]]"}});
}

TEST_F(ListReverseTest, reversesAStoredList) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.tags = [1, 'two', 3]");

    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Adam' RETURN p.name, reverse(p.tags)",
               {{"Remy", "[3, two, 1]"}, {"Adam", "null"}});
}

TEST_F(ListReverseTest, unwindsAReversedList) {
    expectRows("UNWIND reverse([1, 2, 3]) AS x RETURN x", {{"3"}, {"2"}, {"1"}});
}

TEST_F(ListReverseTest, composesWithOtherListFunctions) {
    expectRows("RETURN head(reverse([1, 2, 3])), size(reverse(tail([1, 2, 3])))", {{"3", "2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
