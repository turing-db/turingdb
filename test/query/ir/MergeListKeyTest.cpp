#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A MERGE keyed on a list or a map matches an entity holding an equal one
class MergeListKeyTest : public WriteQueryTest {
};

TEST_F(MergeListKeyTest, matchesTheNodeAnEarlierMergeCreatedWithTheList) {
    applyWrite("MERGE (:Tagged {tags: ['a', 'b']})");
    applyWrite("MERGE (:Tagged {tags: ['a', 'b']})");
    applyWrite("MERGE (:Tagged {tags: ['b', 'a']})");

    expectRows("MATCH (n:Tagged) RETURN count(n)", {{"2"}});
}

TEST_F(MergeListKeyTest, createsOneNodePerDistinctListInOneQuery) {
    applyWrite("UNWIND [['a', 'b'], ['a', 'b'], ['c']] AS t MERGE (:Tagged {tags: t})");

    expectRows("MATCH (n:Tagged) RETURN count(n)", {{"2"}});
}

TEST_F(MergeListKeyTest, matchesTheNodeAnEarlierMergeCreatedWithTheMap) {
    applyWrite("MERGE (:Config {options: {depth: 2, name: 'x'}})");
    applyWrite("MERGE (:Config {options: {name: 'x', depth: 2}})");

    expectRows("MATCH (n:Config) RETURN count(n)", {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
