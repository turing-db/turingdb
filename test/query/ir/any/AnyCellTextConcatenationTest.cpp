#include <gtest/gtest.h>

#include <string_view>

#include "QueryStatus.h"

#include "CallV3Test.h"

using namespace db;
using namespace turing::test;

class AnyCellTextConcatenationTest : public CallV3Test {
protected:
    // A type-erased cell is typed on each row, so the concatenation raises at run time
    void expectRunTimeError(std::string_view query) {
        runQueryExpectingError(query, QueryStatus::Status::EXEC_ERROR, "Operands are not valid and compatible types for '||'");
    }
};

TEST_F(AnyCellTextConcatenationTest, joinsNoNumberCellsAsText) {
    expectRunTimeError("WITH [1, 2, 'a'] AS xs RETURN xs[0] || xs[1], xs[2] || xs[0]");
}

TEST_F(AnyCellTextConcatenationTest, joinsNoListCellToANumberCell) {
    expectRunTimeError("WITH [[1], 2, 'a'] AS xs RETURN xs[0] || xs[1]");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
