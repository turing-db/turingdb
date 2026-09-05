#include <gtest/gtest.h>

#include "QueryCallbacks.h"
#include "QueryState.h"
#include "QueryStatus.h"

#include "TuringDB.h"
#include "dataframe/Dataframe.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The conflict a submit checks a pending edge's ends for, and the edge it must not check:
// one the same query deleted again is never created, so nothing can conflict with it.
class DeletedPendingEdgeConflictTest : public WriteQueryTest {
protected:
    QueryStatus trySubmit(const ChangeID& changeID) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     &callbacks,
                                     CommitHash::head(),
                                     changeID);

        return _env->getDB().query("CHANGE SUBMIT", submitState);
    }
};

// The first change writes an edge off Remy and deletes it again; the second changes Remy
// and submits first. The edge is gone, so its end having moved on main is no conflict.
TEST_F(DeletedPendingEdgeConflictTest, submitsAChangeWhoseWrittenEdgeItDeletedAgain) {
    ChangeID first;
    openChange(first);
    ASSERT_TRUE(runWrite("MATCH (p:Person {name: 'Remy'}) "
                         "CREATE (p)-[e:LINKS]->(b:Tag {name: 'b'}) "
                         "DELETE e",
                         first)
                    .isOk());

    ChangeID second;
    openChange(second);
    ASSERT_TRUE(runWrite("MATCH (p:Person {name: 'Remy'}) SET p.age = 40", second).isOk());

    submit(second);

    const QueryStatus status = trySubmit(first);
    EXPECT_TRUE(status.isOk()) << status.getError();
}

// The same change keeping the edge it wrote: that one does conflict
TEST_F(DeletedPendingEdgeConflictTest, refusesAChangeWhoseWrittenEdgeSurvives) {
    ChangeID first;
    openChange(first);
    ASSERT_TRUE(runWrite("MATCH (p:Person {name: 'Remy'}) "
                         "CREATE (p)-[e:LINKS]->(b:Tag {name: 'b'})",
                         first)
                    .isOk());

    ChangeID second;
    openChange(second);
    ASSERT_TRUE(runWrite("MATCH (p:Person {name: 'Remy'}) SET p.age = 40", second).isOk());

    submit(second);

    const QueryStatus status = trySubmit(first);
    EXPECT_FALSE(status.isOk());
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
