#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A null in a pattern's property map sets nothing: CREATE writes the entity without the
// property, and MERGE, which would match on it, cannot use it
class NullPropertyLiteralTest : public WriteQueryTest {
};

TEST_F(NullPropertyLiteralTest, createsANodeWithoutAPropertyTheGraphDoesNotHave) {
    expectWriteRows("CREATE (g:Gauge {name: 'a', level: null}) RETURN g.name, g.level", {{"a", "null"}});

    expectRows("MATCH (g:Gauge) RETURN g.name, g.level", {{"a", "null"}});
}

TEST_F(NullPropertyLiteralTest, createsANodeWithoutAPropertyTheGraphHas) {
    expectWriteRows("CREATE (p:Person {name: 'Zoe', age: null}) RETURN p.age", {{"null"}});

    expectRows("MATCH (p:Person {name: 'Zoe'}) RETURN p.age", {{"null"}});
}

TEST_F(NullPropertyLiteralTest, createsAnEdgeWithoutTheProperty) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "CREATE (a)-[e:SEES {since: null}]->(b) "
                    "RETURN e.since",
                    {{"null"}});

    expectRows("MATCH (:Person {name: 'Remy'})-[e:SEES]->(b) RETURN b.name, e.since", {{"Adam", "null"}});
}

TEST_F(NullPropertyLiteralTest, rejectsAMergeOnANullProperty) {
    const std::string_view query = "MERGE (g:Gauge {name: null})";

    ChangeID changeID;
    openChange(changeID);

    const QueryStatus status = runWrite(query, changeID);
    EXPECT_FALSE(status.isOk()) << "query: " << query;
    EXPECT_NE(status.getError().find("Cannot merge a node whose property 'name' is null"), std::string::npos)
        << status.getError();

    submit(changeID);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
