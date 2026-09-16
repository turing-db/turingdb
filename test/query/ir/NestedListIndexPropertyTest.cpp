#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An element indexed out of a list UNWIND bound from a list of lists. Every element of the
// outer list is a list whose own elements share one type, so the indexed cell carries that
// type and is a value a property can be written from - the shape LDBC's IU 1 writes a
// STUDY_AT classYear and a WORKS_AT workFrom from.
class NestedListIndexPropertyTest : public WriteQueryTest {
};

TEST_F(NestedListIndexPropertyTest, writesAnIndexedNestedElementAsANodeProperty) {
    expectWriteRowCount("UNWIND [[2435, 2004]] AS s CREATE (o:Org {id: s[0], year: s[1]})", 0);

    expectRows("MATCH (o:Org) RETURN o.id, o.year", {{"2435", "2004"}});
}

TEST_F(NestedListIndexPropertyTest, writesAnIndexedNestedElementAsAnEdgeProperty) {
    expectWriteRowCount("CREATE (a:Org {id: 1}), (b:Org {id: 2})", 0);

    expectWriteRowCount("UNWIND [[1, 2004]] AS s "
                        "MATCH (a:Org {id: s[0]}), (b:Org {id: 2}) "
                        "CREATE (a)-[:STUDY_AT {classYear: s[1]}]->(b)",
                        0);

    expectRows("MATCH (:Org)-[e:STUDY_AT]->(:Org) RETURN e.classYear", {{"2004"}});
}

TEST_F(NestedListIndexPropertyTest, writesOneRowPerElementOfTheOuterList) {
    expectWriteRowCount("UNWIND [[10, 2001], [20, 2002]] AS s CREATE (o:Org {id: s[0], year: s[1]})", 0);

    expectRows("MATCH (o:Org) RETURN o.id, o.year", {{"10", "2001"}, {"20", "2002"}});
}

TEST_F(NestedListIndexPropertyTest, readsAnIndexedNestedElement) {
    expectRows("UNWIND [[2435, 2004]] AS s RETURN s[0], s[1]", {{"2435", "2004"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
