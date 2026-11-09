#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class NormalizeOfANonStringTest : public WriteQueryTest {
protected:
    void expectRejected(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        const std::string& error = status.getError();

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR)
            << "query: " << query << "\nerror: " << error;
        EXPECT_NE(error.find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << error;
    }
};

TEST_F(NormalizeOfANonStringTest, rejectsAnIntegerLiteral) {
    expectRejected("RETURN normalize(1)", "Invalid arguments for function 'normalize': argument 1 cannot be 'Integer'");
}

TEST_F(NormalizeOfANonStringTest, rejectsANonStringInEveryForm) {
    for (const std::string_view form : {"NFC", "NFD", "NFKC", "NFKD"}) {
        expectRejected("RETURN normalize(1, " + std::string(form) + ")",
                       "Invalid arguments for function 'normalize': argument 1 cannot be 'Integer'");
    }
}

TEST_F(NormalizeOfANonStringTest, rejectsEveryValueThatIsNotAString) {
    expectRejected("RETURN normalize(1.5)", "argument 1 cannot be 'Double'");
    expectRejected("RETURN normalize(true, NFD)", "argument 1 cannot be 'Bool'");
    expectRejected("RETURN normalize([1], NFC)", "argument 1 cannot be 'List'");
    expectRejected("RETURN normalize({a: 'b'}, NFKD)", "argument 1 cannot be 'Map'");
    expectRejected("RETURN normalize(datetime('2024-01-01T00:00:00Z'))", "argument 1 cannot be 'DateTime'");
    expectRejected("RETURN normalize(duration(1), NFKC)", "argument 1 cannot be 'Duration'");
    expectRejected("RETURN normalize(point({x: 1.0, y: 2.0}))", "argument 1 cannot be 'Point'");
}

TEST_F(NormalizeOfANonStringTest, rejectsAnEntityOrAPath) {
    expectRejected("MATCH (p:Person {name: 'Remy'}) RETURN normalize(p)", "argument 1 cannot be 'NodePattern'");
    expectRejected("MATCH ()-[e:KNOWS_WELL]->() RETURN normalize(e, NFD)", "argument 1 cannot be 'EdgePattern'");
    expectRejected("MATCH path = (:Person {name: 'Remy'})-[:KNOWS_WELL]->() RETURN normalize(path)",
                   "argument 1 cannot be 'GraphPath'");
}

TEST_F(NormalizeOfANonStringTest, rejectsAPropertyTypedAsANonString) {
    expectRejected("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name, normalize(p.age)",
                   "Invalid arguments for function 'normalize': argument 1 cannot be 'Integer'");
    expectRejected("MATCH (p:Person) RETURN normalize(p.isFrench, NFKC)", "argument 1 cannot be 'Bool'");
    expectRejected("MATCH ()-[e:KNOWS_WELL]->() RETURN normalize(e.duration)", "argument 1 cannot be 'Integer'");
}

TEST_F(NormalizeOfANonStringTest, isNormalizedOfANonStringStaysNull) {
    expectRows("RETURN 1 IS NORMALIZED, 1.5 IS NFD NORMALIZED, [1] IS NOT NFKC NORMALIZED", {{"null", "null", "null"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age IS NORMALIZED, p IS NOT NORMALIZED", {{"null", "null"}});
}

TEST_F(NormalizeOfANonStringTest, otherStringFunctionsNameTheArgumentTheyReject) {
    for (const std::string_view function : {"toUpper", "toLower", "trim", "ltrim", "rtrim", "reverse", "char_length"}) {
        expectRejected("RETURN " + std::string(function) + "(1)",
                       "Invalid arguments for function '" + std::string(function) + "': argument 1 cannot be 'Integer'");
    }

    expectRejected("RETURN substring(1, 0)", "Invalid arguments for function 'substring': argument 1 cannot be 'Integer'");
    expectRejected("RETURN substring('abc', 'b')", "Invalid arguments for function 'substring': argument 2 cannot be 'String'");
    expectRejected("RETURN split(1, ',')", "Invalid arguments for function 'split': argument 1 cannot be 'Integer'");
    expectRejected("RETURN split('a,b', 1)", "Invalid arguments for function 'split': argument 2 cannot be 'Integer'");
    expectRejected("RETURN left(1, 1)", "Invalid arguments for function 'left': argument 1 cannot be 'Integer'");
    expectRejected("RETURN right('abc', 'b')", "Invalid arguments for function 'right': argument 2 cannot be 'String'");
    expectRejected("RETURN replace(1, 'a', 'b')", "Invalid arguments for function 'replace': argument 1 cannot be 'Integer'");
    expectRejected("RETURN replace('abc', 1, 'b')", "Invalid arguments for function 'replace': argument 2 cannot be 'Integer'");
    expectRejected("RETURN replace('abc', 'a', 1)", "Invalid arguments for function 'replace': argument 3 cannot be 'Integer'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
