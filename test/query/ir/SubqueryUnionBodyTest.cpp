#include <gtest/gtest.h>

#include "CallV3Test.h"

using namespace turing::test;

// A UNION inside a subquery body is the construct it is, and is named as unimplemented
// rather than reported as a stray token
class SubqueryUnionBodyTest : public CallV3Test {
};

TEST_F(SubqueryUnionBodyTest, namesTheUnionOfABodyAsUnimplemented) {
    runQueryExpectingError("MATCH (p:Person) "
                           "CALL { MATCH (i:Interest) RETURN i AS z UNION MATCH (q:Person) RETURN q AS z } "
                           "RETURN count(z)",
                           "Not implemented");
}

TEST_F(SubqueryUnionBodyTest, namesTheUnionOfAScopedBodyAsUnimplemented) {
    runQueryExpectingError("MATCH (p:Person) "
                           "CALL (p) { MATCH (p)-->(x) RETURN x AS z UNION MATCH (q:Person) RETURN q AS z } "
                           "RETURN count(z)",
                           "Not implemented");
}
