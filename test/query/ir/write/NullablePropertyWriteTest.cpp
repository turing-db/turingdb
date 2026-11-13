#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class NullablePropertyWriteTest : public WriteQueryTest {
};

TEST_F(NullablePropertyWriteTest, writesANullableListPerRow) {
    applyWrite("MATCH (n:Person) SET n.tags = CASE WHEN n.name = 'Remy' THEN [1, 2] ELSE null END");
    expectRows("MATCH (n:Person) WHERE n.tags IS NOT NULL RETURN n.name, n.tags", {{"Remy", "[1, 2]"}});
}

TEST_F(NullablePropertyWriteTest, writesANullableMapPerRow) {
    applyWrite("MATCH (n:Person) SET n.attrs = CASE WHEN n.name = 'Adam' THEN {a: 1} ELSE null END");
    expectRows("MATCH (n:Person) WHERE n.attrs IS NOT NULL RETURN n.name, n.attrs.a", {{"Adam", "1"}});
}

TEST_F(NullablePropertyWriteTest, writesANullableStringPerRow) {
    applyWrite("MATCH (n:Person) SET n.nick = CASE WHEN n.name = 'Remy' THEN 'R' ELSE null END");
    expectRows("MATCH (n:Person) WHERE n.nick IS NOT NULL RETURN n.name, n.nick", {{"Remy", "R"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
