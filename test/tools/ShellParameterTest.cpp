#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "ShellTestFixture.h"

using namespace turing::test;

class ShellParameterTest : public ShellTestFixture {
protected:
    // The first cell of the row under the table header @param header, two lines below it,
    // or nothing when no such row exists
    static void firstCellUnder(const std::string& output, std::string_view header, std::string& cell) {
        cell.clear();

        size_t position = output.find(header);
        for (size_t line = 0; line < 2 && position != std::string::npos; line++) {
            position = output.find('\n', position + 1);
        }

        const size_t bar = position == std::string::npos ? std::string::npos : output.find("| ", position);
        if (bar == std::string::npos) {
            return;
        }

        const size_t start = bar + 2;
        const size_t end = output.find(' ', start);
        cell = output.substr(start, end - start);
    }
};

TEST_F(ShellParameterTest, aLineCommentInTheExpressionIsIgnored) {
    std::string output;
    runLines(":param x => 1 // the answer\nRETURN $x AS x\n", output);

    EXPECT_NE(output.find("| 1 |"), std::string::npos) << output;
}

TEST_F(ShellParameterTest, aTrailingSemicolonIsAccepted) {
    std::string output;
    runLines(":param x => 1;\nRETURN $x AS x\n", output);

    EXPECT_NE(output.find("| 1 |"), std::string::npos) << output;
}

TEST_F(ShellParameterTest, aSemicolonBeforeACommentIsAccepted) {
    std::string output;
    runLines(":param x => 1; // the answer\nRETURN $x AS x\n", output);

    EXPECT_NE(output.find("| 1 |"), std::string::npos) << output;
}

TEST_F(ShellParameterTest, anExpressionCannotEscapeItsParentheses) {
    std::string output;
    runLines(":param y => 1) AS a, (2\n:param z => 2\nSHOW PARAMETERS\n", output);

    EXPECT_NE(output.find("| z "), std::string::npos) << output;
    EXPECT_EQ(output.find("| y "), std::string::npos) << output;
}

TEST_F(ShellParameterTest, anEscapedExpressionRunsNothing) {
    std::string output;
    runLines("CHANGE NEW\n"
             "checkout change-0\n"
             ":param u => 1) AS value UNION CREATE (:Escaped) RETURN (2\n"
             ":param n => 1) AS value NEXT CREATE (:Escaped) RETURN (2\n"
             "COMMIT\n"
             "MATCH (e:Escaped) RETURN count(e) AS escaped\n",
             output);

    std::string escaped;
    firstCellUnder(output, "| escaped |", escaped);
    EXPECT_EQ(escaped, "0") << output;
}
