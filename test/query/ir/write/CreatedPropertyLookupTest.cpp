#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A property the query itself creates, read back off the value of an expression rather
// than off a variable: the graph's schema does not carry the name until the commit.
class CreatedPropertyLookupTest : public WriteQueryTest {
};

TEST_F(CreatedPropertyLookupTest, readsAKeyOfAMapPropertyASetCreated) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "SET a.attrs = {w: 7} "
                    "CREATE (a)-[e:TAGGED]->(b) "
                    "WITH 1 AS seeded "
                    "MATCH ()-[r:TAGGED]->() "
                    "RETURN startNode(r).attrs.w",
                    {{"7"}});
}

TEST_F(CreatedPropertyLookupTest, readsAComputedKeyOfAMapPropertyASetCreated) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "SET a.attrs = {w: 7} "
                    "CREATE (a)-[e:TAGGED]->(b) "
                    "WITH 1 AS seeded "
                    "MATCH ()-[r:TAGGED]->() "
                    "WITH r, 'w' AS k "
                    "RETURN startNode(r).attrs[k]",
                    {{"7"}});
}

TEST_F(CreatedPropertyLookupTest, readsAnIntegerPropertyASetCreated) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "SET a.score = 7 "
                    "CREATE (a)-[e:TAGGED]->(b) "
                    "WITH 1 AS seeded "
                    "MATCH ()-[r:TAGGED]->() "
                    "RETURN startNode(r).score",
                    {{"7"}});
}

TEST_F(CreatedPropertyLookupTest, readsAComponentOfADateTimePropertyASetCreated) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "SET a.born = datetime('2001-02-03T00:00:00Z') "
                    "CREATE (a)-[e:TAGGED]->(b) "
                    "WITH 1 AS seeded "
                    "MATCH ()-[r:TAGGED]->() "
                    "RETURN startNode(r).born.year",
                    {{"2001"}});
}

TEST_F(CreatedPropertyLookupTest, readsANodePropertyACreatePatternCreated) {
    expectWriteRows("CREATE (:Tag {weight: 5}) "
                    "WITH 1 AS seeded "
                    "MATCH (t:Tag) "
                    "RETURN head(collect(t)).weight",
                    {{"5"}});
}

TEST_F(CreatedPropertyLookupTest, readsAnEdgePropertyACreatePatternCreated) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "CREATE (a)-[e:TAGGED {weight: 3}]->(b) "
                    "WITH 1 AS seeded "
                    "MATCH ()-[r:TAGGED]->() "
                    "RETURN head(collect(r)).weight",
                    {{"3"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
