#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A string function or an arithmetic operator a WITH projects raises on a type-erased cell
// holding a value of another type, though nothing reads it after. Over a typed column it
// cannot raise, so nothing keeps it.
class UnreadHeldTypeErrorRaisesTest : public WriteQueryTest {
protected:
    void optimisedProgram(std::string_view query, std::string& program) {
        RowSink sink;
        const QueryStatus status = runQuery("EXPLAIN (db) " + std::string(query), &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        for (const Row& row : sink.rows()) {
            if (row.front() == "db") {
                program = row.back();
            }
        }

        ASSERT_FALSE(program.empty()) << "query: " << query;
    }

    void expectKept(std::string_view query, std::string_view op) {
        std::string program;
        optimisedProgram(query, program);

        EXPECT_NE(program.find(op), std::string::npos) << program;
    }

    void expectErased(std::string_view query, std::string_view op) {
        std::string program;
        optimisedProgram(query, program);

        EXPECT_EQ(program.find(op), std::string::npos) << program;
    }
};

TEST_F(UnreadHeldTypeErrorRaisesTest, aStringFunctionOfACellHoldingNoString) {
    expectError("UNWIND ['a', 1] AS x WITH x, toUpper(x) AS unused RETURN count(*)", "toUpper() reads a string, not 'Integer'");
    expectError("UNWIND ['a', 1] AS x WITH x, normalize(x) AS unused RETURN count(*)", "normalize() reads a string, not 'Integer'");
    expectError("UNWIND ['a', 1] AS x WITH x, replace(x, 'a', 'b') AS unused RETURN count(*)", "replace() reads a string, not 'Integer'");
    expectError("UNWIND ['a', 1] AS x WITH x, left(x, 1) AS unused RETURN count(*)", "left() reads a string, not 'Integer'");
}

TEST_F(UnreadHeldTypeErrorRaisesTest, anArithmeticOperatorOverACellHoldingNoNumber) {
    expectError("UNWIND [1, 'a'] AS x WITH x, x - 1 AS unused RETURN count(*)",
                "Operands are not valid and compatible numeric types: 'String' and 'Integer'");
    expectError("UNWIND [1, 'a'] AS x WITH x, x ^ 2 AS unused RETURN count(*)",
                "Operands are not valid and compatible numeric types: 'String' and 'Integer'");
}

TEST_F(UnreadHeldTypeErrorRaisesTest, returnsEveryRowPastAFunctionOfTypedValues) {
    expectRows("MATCH (n:Person) WITH n, toUpper(n.name) AS unused RETURN count(*)", {{"8"}});
    expectRows("MATCH (n:Person) WITH n, normalize(n.name) AS unused RETURN count(*)", {{"8"}});
    expectRows("MATCH (n:Person) WITH n, n.age ^ 2 AS unused RETURN count(*)", {{"8"}});
}

TEST_F(UnreadHeldTypeErrorRaisesTest, readsNoCellInABranchNoRowTakes) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, CASE WHEN p.age > 1000 THEN normalize([1, 'a'][0]) ELSE 'none' END",
               {{"Luc", "none"}, {"Remy", "none"}});
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, CASE WHEN p.age > 1000 THEN toUpper([1, 'a'][0]) ELSE 'none' END",
               {{"Luc", "none"}, {"Remy", "none"}});
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, CASE WHEN p.age > 1000 THEN ['a', 1][0] - 1 ELSE 0 END",
               {{"Luc", "0"}, {"Remy", "0"}});
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, CASE WHEN p.age > 1000 THEN ['a', 1][0] ^ 2 ELSE 0 END",
               {{"Luc", "0"}, {"Remy", "0"}});
}

TEST_F(UnreadHeldTypeErrorRaisesTest, keepsAnUnreadOpOverACell) {
    expectKept("UNWIND ['a', 1] AS x WITH x, toUpper(x) AS unused RETURN count(*)", "db.to_upper");
    expectKept("UNWIND ['a', 1] AS x WITH x, normalize(x) AS unused RETURN count(*)", "db.normalize");
    expectKept("UNWIND [1, 'a'] AS x WITH x, x - 1 AS unused RETURN count(*)", "db.sub");
    expectKept("UNWIND [1, 'a'] AS x WITH x, x ^ 2 AS unused RETURN count(*)", "db.pow");
}

TEST_F(UnreadHeldTypeErrorRaisesTest, erasesAnUnreadOpOverTypedValues) {
    expectErased("MATCH (n:Person) WITH n, toUpper(n.name) AS unused RETURN count(*)", "db.to_upper");
    expectErased("MATCH (n:Person) WITH n, n.age - 1 AS unused RETURN count(*)", "db.sub");
    expectErased("MATCH (n:Person) WITH n, n.age ^ 2 AS unused RETURN count(*)", "db.pow");
}
