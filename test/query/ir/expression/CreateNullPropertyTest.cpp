#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class CreateNullPropertyTest : public WriteQueryTest {
};

TEST_F(CreateNullPropertyTest, createsANodeWithoutANullLiteralProperty) {
    applyWrite("CREATE (:Tag {name: 'a', hasPhD: null})");

    expectRows("MATCH (t:Tag) RETURN t.name, t.hasPhD", {{"a", "null"}});
}

TEST_F(CreateNullPropertyTest, createsANodeWithoutANullComparisonOnANewKey) {
    applyWrite("CREATE (:Tag {name: 'a', flag: 1 = null})");

    expectRows("MATCH (t:Tag) RETURN t.name, t.flag", {{"a", "null"}});
}

TEST_F(CreateNullPropertyTest, createsANodeWithoutAnOrderingAcrossTypesOnAnExistingKey) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) CREATE (:Tag {name: 'a', hasPhD: n.name < 5})");

    expectRows("MATCH (t:Tag) RETURN t.name, t.hasPhD", {{"a", "null"}});
}

TEST_F(CreateNullPropertyTest, createsAnEdgeWithoutAnOrderingAcrossTypes) {
    applyWrite("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "CREATE (a)-[:TAGGED {name: 'e', duration: 1 < 'a'}]->(b)");

    expectRows("MATCH ()-[e:TAGGED]->() RETURN e.name, e.duration", {{"e", "null"}});
}

TEST_F(CreateNullPropertyTest, mergeRejectsANullProperty) {
    db::ChangeID changeID;
    openChange(changeID);

    const db::QueryStatus status = runWrite("MERGE (:Tag {name: 'a', flag: 1 = null})", changeID);
    EXPECT_FALSE(status.isOk());
    EXPECT_NE(status.getError().find("null"), std::string::npos) << status.getError();
}
