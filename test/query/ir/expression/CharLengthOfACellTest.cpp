#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class CharLengthOfACellTest : public WriteQueryTest {
};

TEST_F(CharLengthOfACellTest, aStringCellCountsItsCharacters) {
    expectRows("UNWIND ['ab', 'été', null] AS v RETURN char_length(v), character_length(v)",
               {{"2", "2"}, {"3", "3"}, {"null", "null"}});
}

TEST_F(CharLengthOfACellTest, aListCellIsATypeError) {
    expectError("UNWIND [[1, 2, 3], 'ab'] AS v RETURN char_length(v)", "char_length");
    expectError("UNWIND [[1, 2, 3], 'ab'] AS v RETURN character_length(v)", "char_length");
    expectError("UNWIND [7, 'ab'] AS v RETURN char_length(v)", "char_length");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
