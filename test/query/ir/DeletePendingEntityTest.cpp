#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A commit groups the nodes it writes by label set, renumbering them, and orders its edges
// by source: the tombstone of an entity it creates and deletes again must name that entity
// after the renumbering, not the one standing at its place before
class DeletePendingEntityTest : public WriteQueryTest {
protected:
    void writeInOneChange(std::initializer_list<std::string_view> queries) {
        ChangeID changeID;
        openChange(changeID);

        for (const std::string_view query : queries) {
            const QueryStatus status = runWrite(query, changeID);
            EXPECT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
        }

        submit(changeID);
    }
};

TEST_F(DeletePendingEntityTest, deletesOnlyTheNodeTheQueryCreated) {
    writeInOneChange({"CREATE (:Person {name: 'a'}), (:Pet {name: 'b'})",
                      "CREATE (p:Person {name: 'Zed'}) DELETE p"});

    expectRows("MATCH (n:Pet) RETURN n.name", {{"b"}});
    expectRows("MATCH (n:Person) WHERE n.name IN ['a', 'Zed'] OR n.name IS NULL RETURN n.name", {{"a"}});
}

TEST_F(DeletePendingEntityTest, keepsTheEdgesOfTheNodesAnEarlierQueryCreated) {
    writeInOneChange({"CREATE (:Person {name: 'owner'})-[:OWNS]->(:Pet {name: 'pet'})",
                      "CREATE (p:Person {name: 'Zed'}) DELETE p"});

    expectRows("MATCH (:Person {name: 'owner'})-[:OWNS]->(t) RETURN t.name", {{"pet"}});
    expectRows("MATCH (n) RETURN count(n)", {{"20"}});
}

TEST_F(DeletePendingEntityTest, deletesOnlyTheEdgeTheQueryCreated) {
    writeInOneChange({"CREATE (a:Person {name: 'ea'})-[r:LIKES]->(b:Pet {name: 'eb'}), (b)-[:SEES]->(a) DELETE r"});

    expectRows("MATCH (:Pet {name: 'eb'})-[s:SEES]->(a) RETURN a.name", {{"ea"}});
    expectRows("MATCH (:Person {name: 'ea'})-[r:LIKES]->() RETURN count(r)", {{"0"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
