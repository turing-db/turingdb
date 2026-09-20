#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A comprehension body that names the node a CREATE in the same query wrote. The created
// entity's column is in flight like a matched one, so the body reads the node of its own
// row rather than whichever row sits at the element's index.
class ListComprehensionCreatedEntityTest : public WriteQueryTest {
};

TEST_F(ListComprehensionCreatedEntityTest, readsAPropertyOfTheNodeItCreated) {
    expectWriteRows("CREATE (n:Tag {name: 'x'}) RETURN [y IN [1,2] | n.name]", {{"[x, x]"}});
}

TEST_F(ListComprehensionCreatedEntityTest, readsThePropertyOnEveryRowItCreated) {
    expectWriteRows("UNWIND ['a','b'] AS i CREATE (n:Tag {name: i}) RETURN [y IN [1,2] | n.name]",
                    {{"[a, a]"},
                     {"[b, b]"}});
}

TEST_F(ListComprehensionCreatedEntityTest, filtersOnAPropertyOfTheNodeItCreated) {
    expectWriteRows("CREATE (n:Tag {name: 'x'}) RETURN [y IN ['x','z'] WHERE y = n.name]",
                    {{"[x]"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
