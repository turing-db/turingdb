#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A MERGE keyed on the element of a list that mixes types keys each row on the value it
// would write: the cell staged as the property's type
class MergeTaggedCellKeyTest : public WriteQueryTest {
protected:
    void expectRejected(std::string_view query, std::string_view message) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(MergeTaggedCellKeyTest, mergesNothingOverAnEmptyList) {
    expectWriteRows("UNWIND [] AS x MERGE (u:User {uid: x}) RETURN count(*)", {{"0"}});

    expectRows("MATCH (u:User) RETURN count(u)", {{"0"}});
}

TEST_F(MergeTaggedCellKeyTest, upsertsOnAPropertyTheGraphDoesNotHave) {
    applyWrite("UNWIND [[1, 'a'], [2, 'b'], [1, 'c']] AS r MERGE (u:User {uid: r[0]}) SET u.name = r[1]");

    expectRows("MATCH (u:User) RETURN u.uid, u.name", {{"1", "c"}, {"2", "b"}});
}

TEST_F(MergeTaggedCellKeyTest, matchesAPropertyTheGraphHas) {
    expectWriteRows("UNWIND [['Remy', 1], ['Zoe', 2]] AS r MERGE (p:Person {name: r[0]}) RETURN p.name",
                    {{"Remy"}, {"Zoe"}});

    expectRows("MATCH (p:Person) RETURN count(p)", {{"9"}});
}

TEST_F(MergeTaggedCellKeyTest, mergesAnEdgeKeyedOnAnElement) {
    applyWrite("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "UNWIND [[1, 'x'], [1, 'y']] AS r "
               "MERGE (a)-[e:T {k: r[0]}]->(b)");

    expectRows("MATCH (:Person {name: 'Remy'})-[e:T]->(:Person {name: 'Adam'}) RETURN e.k, count(e)", {{"1", "1"}});
}

TEST_F(MergeTaggedCellKeyTest, rejectsAnElementOfAnotherTypeThanTheProperty) {
    expectRejected("UNWIND [[1, 'a'], ['x', 'b']] AS r MERGE (u:User {uid: r[0]})",
                   "Cannot write a value of another type");
}

TEST_F(MergeTaggedCellKeyTest, rejectsANullElement) {
    expectRejected("UNWIND [[null, 'a']] AS r MERGE (u:User {uid: r[0]})",
                   "Cannot merge a node whose property 'uid' is null");
}

TEST_F(MergeTaggedCellKeyTest, matchesTheKeyAnOnMatchOfAnEarlierRowRewrote) {
    applyWrite("UNWIND [[1, 'a'], [1, 'b'], [2, 'c']] AS r MERGE (u:K {k: r[0]}) ON MATCH SET u.k = 2");

    expectRows("MATCH (u:K) RETURN u.k", {{"2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
