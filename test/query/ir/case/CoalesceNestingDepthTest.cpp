#include <gtest/gtest.h>

#include <stddef.h>

#include <string>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A coalesce nested in the first argument of another, forty deep, compiles and runs in the
// time a single level takes forty times over. Remy and Adam are the people with an age.
class CoalesceNestingDepthTest : public WriteQueryTest {
};

TEST_F(CoalesceNestingDepthTest, nestsFortyCoalesces) {
    constexpr size_t NESTING_DEPTH = 40;

    std::string nested = "n.age";
    for (size_t level = 0; level < NESTING_DEPTH; level++) {
        nested = "coalesce(" + nested + ", n.age)";
    }

    expectRows("MATCH (n:Person) RETURN n.name, coalesce(" + nested + ", -1)",
               {{"Adam", "32"}, {"Cyrus", "-1"}, {"Doruk", "-1"}, {"Luc", "-1"},
                {"Martina", "-1"}, {"Maxime", "-1"}, {"Remy", "32"}, {"Suhas", "-1"}});
}
