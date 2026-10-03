#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A comprehension over the list a node holds as a property: its elements are type-erased
// cells, read out of the nullable list column a property fetch produces.
class ListPropertyComprehensionTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("CREATE (a:Tagged {name: 'a', tags: ['Java', 'Python']})");
        applyWrite("CREATE (b:Tagged {name: 'b', tags: [1, 2, 3]})");
        applyWrite("CREATE (c:Tagged {name: 'c'})");
    }
};

TEST_F(ListPropertyComprehensionTest, yieldsTheElementsOfAStoredList) {
    expectRows("MATCH (n:Tagged {name: 'a'}) RETURN [tag IN n.tags | tag]",
               {{"[Java, Python]"}});
}

TEST_F(ListPropertyComprehensionTest, concatenatesOntoTheElementsOfAStoredList) {
    expectRows("MATCH (n:Tagged {name: 'a'}) RETURN [tag IN n.tags | tag + ' expert']",
               {{"[Java expert, Python expert]"}});
}

TEST_F(ListPropertyComprehensionTest, concatenatesOntoTheStoredListsOfEveryRow) {
    expectRows("MATCH (n:Tagged) RETURN n.name, [tag IN n.tags | tag + '!']",
               {{"a", "[Java!, Python!]"}, {"b", "[1!, 2!, 3!]"}, {"c", "null"}});
}

TEST_F(ListPropertyComprehensionTest, filtersTheElementsOfAStoredList) {
    expectRows("MATCH (n:Tagged {name: 'b'}) RETURN [tag IN n.tags WHERE tag > 1 | tag]",
               {{"[2, 3]"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
