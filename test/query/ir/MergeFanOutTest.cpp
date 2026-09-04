#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A merge emits one row per match it binds, so the rows in flight around it grow. What
// every column beside it has to look like once they have.
class MergeFanOutTest : public WriteQueryTest {
};

// The value the first merge wrote for t.name is a column of the rows that reached it,
// and the second merge fans those rows out over the eight Persons it binds: the
// projection reads the property off that column, so it has to be re-indexed onto the
// emitted rows along with t's own ID column
TEST_F(MergeFanOutTest, carriesAWrittenPropertyPastAMergeThatFansTheRowsOut) {
    const Rows remyPerPerson(8, Row {"Remy"});

    expectWriteRows("MATCH (p:Person {name: 'Remy'}) "
                    "MERGE (t:Tag {name: p.name}) "
                    "MERGE (q:Person) "
                    "RETURN t.name",
                    remyPerPerson);

    expectRows("MATCH (t:Tag) RETURN t.name", {{"Remy"}});
}

// The same fan-out, read through the merge's own entity rather than its property: one
// row per Person, each naming the single Tag the first merge wrote
TEST_F(MergeFanOutTest, carriesAWrittenEntityPastAMergeThatFansTheRowsOut) {
    expectWriteRowCount("MATCH (p:Person {name: 'Remy'}) "
                        "MERGE (t:Tag {name: p.name}) "
                        "MERGE (q:Person) "
                        "RETURN t.name, q.name",
                        8);

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

// The CREATE comes first in the query, so it writes over the one row the query starts
// with - not over the eight the merge behind it fans out to
TEST_F(MergeFanOutTest, writesACreateAheadOfAMergeOverTheRowsAheadOfIt) {
    expectWriteRowCount("CREATE (t:Tag {name: 'x'}) MERGE (q:Person)", 0);

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

// The other way round: the merge fans the rows out first, so the CREATE behind it writes
// one node per row it emitted
TEST_F(MergeFanOutTest, writesACreateBehindAMergeOverTheRowsItEmitted) {
    expectWriteRowCount("MERGE (q:Person) CREATE (t:Tag {name: 'x'})", 0);

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"8"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
