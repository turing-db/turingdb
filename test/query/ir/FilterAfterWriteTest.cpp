#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A WHERE after an updating clause filters the rows that clause left, as it wrote them: it
// neither runs ahead of the write nor stands for a fresh scan of the graph. Remy is 32.
class FilterAfterWriteTest : public WriteQueryTest {
};

TEST_F(FilterAfterWriteTest, filtersOnTheValueTheSetWrote) {
    expectWriteRows("UNWIND [1] AS x MATCH (n:Person {name: 'Remy'}) SET n.age = 40 WITH n WHERE n.age = 40 RETURN n.age",
                    {{"40"}});

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age", {{"40"}});
}

TEST_F(FilterAfterWriteTest, setsEveryRowBeforeAWhereOnAnotherProperty) {
    expectWriteRows("MATCH (n:Person) SET n.visited = 1 WITH n WHERE n.name = 'Remy' RETURN n.name", {{"Remy"}});

    expectRows("MATCH (n:Person) WHERE n.visited = 1 RETURN count(n)", {{"8"}});
}

TEST_F(FilterAfterWriteTest, keepsEveryUnwoundRowAWhereOnTheWrittenValueLetsThrough) {
    expectWriteRows("UNWIND [1, 2] AS i MATCH (n:Person) SET n.age = 40 WITH n WHERE n.age = 40 RETURN count(n)",
                    {{"16"}});
}

TEST_F(FilterAfterWriteTest, filtersOnAPropertyTheSetCreatedFromAListElement) {
    expectWriteRows("MATCH (n:Person) SET n.level = ['a', 5][1] WITH n WHERE n.level = 5 RETURN count(n)", {{"8"}});
}

TEST_F(FilterAfterWriteTest, filtersOnALabelAfterASet) {
    expectWriteRows("MATCH (n) SET n.seen = true WITH n WHERE n:Interest RETURN count(n)", {{"10"}});

    expectRows("MATCH (n) WHERE n.seen = true RETURN count(n)", {{"18"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
