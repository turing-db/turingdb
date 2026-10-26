#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

const std::string COMPOSED_E_ACUTE = "\xC3\xA9";
const std::string DECOMPOSED_E_ACUTE = "e\xCC\x81";
const std::string FI_LIGATURE = "\xEF\xAC\x81";
const std::string ANGSTROM_SIGN = "\xE2\x84\xAB";

std::string stringLiteral(std::string_view text) {
    return "'" + std::string(text) + "'";
}

}

class IsNormalizedPredicateTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.nick = 'R" + COMPOSED_E_ACUTE + "my', "
                   "p.words = [" + stringLiteral(DECOMPOSED_E_ACUTE) + ", 3, null, " + stringLiteral(COMPOSED_E_ACUTE) + "]");
        applyWrite("MATCH (p:Person {name: 'Adam'}) SET p.nick = '" + FI_LIGATURE + "ne'");
        applyWrite("MATCH (p:Person {name: 'Luc'}) SET p.nick = 'Lu" + DECOMPOSED_E_ACUTE + "'");
    }
};

TEST_F(IsNormalizedPredicateTest, composedInput) {
    const std::string s = stringLiteral(COMPOSED_E_ACUTE);
    expectRows("RETURN " + s + " IS NFC NORMALIZED, " + s + " IS NFD NORMALIZED, "
               + s + " IS NFKC NORMALIZED, " + s + " IS NFKD NORMALIZED",
               {{"true", "false", "true", "false"}});
}

TEST_F(IsNormalizedPredicateTest, decomposedInput) {
    const std::string s = stringLiteral(DECOMPOSED_E_ACUTE);
    expectRows("RETURN " + s + " IS NFC NORMALIZED, " + s + " IS NFD NORMALIZED, "
               + s + " IS NFKC NORMALIZED, " + s + " IS NFKD NORMALIZED",
               {{"false", "true", "false", "true"}});
}

TEST_F(IsNormalizedPredicateTest, compatibilityInput) {
    const std::string s = stringLiteral(FI_LIGATURE);
    expectRows("RETURN " + s + " IS NFC NORMALIZED, " + s + " IS NFD NORMALIZED, "
               + s + " IS NFKC NORMALIZED, " + s + " IS NFKD NORMALIZED",
               {{"true", "true", "false", "false"}});
}

TEST_F(IsNormalizedPredicateTest, defaultFormIsNFC) {
    expectRows("RETURN " + stringLiteral(COMPOSED_E_ACUTE) + " IS NORMALIZED, " + stringLiteral(DECOMPOSED_E_ACUTE) + " IS NORMALIZED, "
               + stringLiteral(FI_LIGATURE) + " IS NORMALIZED, 'the " + ANGSTROM_SIGN + " char' IS NORMALIZED",
               {{"true", "false", "true", "false"}});
}

TEST_F(IsNormalizedPredicateTest, isNotNegates) {
    expectRows("RETURN " + stringLiteral(COMPOSED_E_ACUTE) + " IS NOT NORMALIZED, " + stringLiteral(DECOMPOSED_E_ACUTE) + " IS NOT NORMALIZED, "
               + stringLiteral(COMPOSED_E_ACUTE) + " IS NOT NFD NORMALIZED, " + stringLiteral(FI_LIGATURE) + " IS NOT NFKD NORMALIZED",
               {{"false", "true", "true", "true"}});
}

TEST_F(IsNormalizedPredicateTest, keywordsAreCaseInsensitive) {
    expectRows("RETURN " + stringLiteral(DECOMPOSED_E_ACUTE) + " is normalized, " + stringLiteral(DECOMPOSED_E_ACUTE) + " Is Nfd Normalized, "
               + stringLiteral(FI_LIGATURE) + " is not nfkc normalized, " + stringLiteral(COMPOSED_E_ACUTE) + " iS nOt NfC nOrMaLiZeD",
               {{"false", "true", "true", "false"}});
}

TEST_F(IsNormalizedPredicateTest, asciiAndEmptyStringsAreInEveryForm) {
    expectRows("RETURN 'hello' IS NORMALIZED, 'a b' IS NFD NORMALIZED, 'fi' IS NFKC NORMALIZED, '' IS NFKD NORMALIZED, "
               "'' IS NOT NORMALIZED",
               {{"true", "true", "true", "true", "false"}});
}

TEST_F(IsNormalizedPredicateTest, nullIsNull) {
    expectRows("RETURN null IS NORMALIZED, null IS NFD NORMALIZED, null IS NOT NFKC NORMALIZED", {{"null", "null", "null"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.nosuch IS NORMALIZED, p.nosuch IS NOT NFKD NORMALIZED",
               {{"null", "null"}});
    expectRows("UNWIND [" + stringLiteral(DECOMPOSED_E_ACUTE) + ", null] AS s RETURN s IS NORMALIZED, s IS NOT NORMALIZED",
               {{"false", "true"}, {"null", "null"}});
}

TEST_F(IsNormalizedPredicateTest, nonStringIsNull) {
    expectRows("RETURN 1 IS NORMALIZED, 1.5 IS NFD NORMALIZED, true IS NOT NORMALIZED, [1] IS NFKC NORMALIZED, "
               "{a: 'b'} IS NORMALIZED",
               {{"null", "null", "null", "null", "null"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p IS NORMALIZED", {{"null"}});
}

TEST_F(IsNormalizedPredicateTest, propertyPerRow) {
    expectRows("MATCH (p:Person) WHERE p.nick IS NOT NULL "
               "RETURN p.name, p.nick IS NORMALIZED, p.nick IS NFD NORMALIZED, p.nick IS NFKC NORMALIZED, p.nick IS NFKD NORMALIZED",
               {{"Remy", "true", "false", "true", "false"},
                {"Adam", "true", "true", "false", "false"},
                {"Luc", "false", "true", "false", "true"}});
    expectRows("MATCH (p:Person) RETURN p.name, p.nick IS NOT NFC NORMALIZED",
               {{"Remy", "false"}, {"Adam", "false"}, {"Luc", "true"}, {"Maxime", "null"}, {"Martina", "null"},
                {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

TEST_F(IsNormalizedPredicateTest, predicateInWhere) {
    expectRows("MATCH (p:Person) WHERE p.nick IS NFD NORMALIZED RETURN p.name", {{"Adam"}, {"Luc"}});
    expectRows("MATCH (p:Person) WHERE p.nick IS NOT NORMALIZED RETURN p.name", {{"Luc"}});
    expectRows("MATCH (p:Person) WHERE p.nick IS NFKC NORMALIZED AND p.nick IS NOT NFKD NORMALIZED RETURN p.name", {{"Remy"}});
    expectRows("MATCH (p:Person) WHERE NOT p.nick IS NFKC NORMALIZED RETURN p.name", {{"Adam"}, {"Luc"}});
}

TEST_F(IsNormalizedPredicateTest, constantOverEveryRow) {
    expectRows("MATCH (p:Person) RETURN " + stringLiteral(FI_LIGATURE) + " IS NFKC NORMALIZED, count(p)", {{"false", "8"}});
    expectRows("MATCH (p:Person) WHERE " + stringLiteral(COMPOSED_E_ACUTE) + " IS NORMALIZED RETURN count(p)", {{"8"}});
    expectRows("MATCH (p:Person) WHERE " + stringLiteral(DECOMPOSED_E_ACUTE) + " IS NORMALIZED RETURN count(p)", {{"0"}});
}

TEST_F(IsNormalizedPredicateTest, typeErasedCells) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "RETURN p.words[0] IS NORMALIZED, p.words[0] IS NFD NORMALIZED, p.words[3] IS NORMALIZED, "
               "p.words[3] IS NOT NFKD NORMALIZED, p.words[1] IS NORMALIZED, p.words[2] IS NORMALIZED",
               {{"false", "true", "true", "true", "null", "null"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) UNWIND p.words AS w RETURN w IS NFD NORMALIZED",
               {{"true"}, {"null"}, {"null"}, {"false"}});
}

TEST_F(IsNormalizedPredicateTest, agreesWithNormalize) {
    expectRows("UNWIND [" + stringLiteral(COMPOSED_E_ACUTE) + ", " + stringLiteral(DECOMPOSED_E_ACUTE) + ", " + stringLiteral(FI_LIGATURE) + ", "
               "'the " + ANGSTROM_SIGN + " char', 'plain', ''] AS s "
               "RETURN (s IS NFC NORMALIZED) = (normalize(s, NFC) = s), (s IS NFD NORMALIZED) = (normalize(s, NFD) = s), "
               "(s IS NFKC NORMALIZED) = (normalize(s, NFKC) = s), (s IS NFKD NORMALIZED) = (normalize(s, NFKD) = s)",
               {{"true", "true", "true", "true"},
                {"true", "true", "true", "true"},
                {"true", "true", "true", "true"},
                {"true", "true", "true", "true"},
                {"true", "true", "true", "true"},
                {"true", "true", "true", "true"}});
    expectRows("UNWIND [" + stringLiteral(DECOMPOSED_E_ACUTE) + ", " + stringLiteral(FI_LIGATURE) + "] AS s "
               "RETURN normalize(s, NFKC) IS NFKC NORMALIZED, normalize(s, NFD) IS NFD NORMALIZED",
               {{"true", "true"}, {"true", "true"}});
}

TEST_F(IsNormalizedPredicateTest, combinesWithComparisonsAndNullTests) {
    expectRows("RETURN 'abc' IS NORMALIZED = true, " + stringLiteral(DECOMPOSED_E_ACUTE) + " IS NOT NFC NORMALIZED IS NULL, "
               "null IS NORMALIZED IS NULL, 1 = 1 AND " + stringLiteral(DECOMPOSED_E_ACUTE) + " IS NFD NORMALIZED, "
               "'a' IS NULL, 'a' IS NOT NULL",
               {{"true", "false", "true", "true", "false", "true"}});
}

TEST_F(IsNormalizedPredicateTest, labelTestsStillWork) {
    expectRows("MATCH (n) WHERE n:Person AND n.nick IS NORMALIZED RETURN n.name", {{"Remy"}, {"Adam"}});
    expectRows("MATCH (n:Person) WHERE n.nick IS NOT NULL RETURN n.name, n:Founder", {{"Remy", "true"}, {"Adam", "true"}, {"Luc", "false"}});
}

TEST_F(IsNormalizedPredicateTest, formNamesRemainUsableAsNames) {
    expectRows("WITH 'x' AS nfc, " + stringLiteral(COMPOSED_E_ACUTE) + " AS nfd, 'z' AS nfkd RETURN nfc IS NORMALIZED, nfd IS NFD NORMALIZED, nfkd",
               {{"true", "false", "z"}});
    expectRows("WITH 'a' AS nfc RETURN nfc IS NULL, nfc IS NOT NULL", {{"false", "true"}});
    expectRows("WITH {normalized: 1, NFC: 2, nfkd: 3} AS m RETURN m.normalized, m.NFC, m.nfkd", {{"1", "2", "3"}});
    expectRows("WITH 'x' AS `normalized` RETURN `normalized` IS NORMALIZED, `normalized`", {{"true", "x"}});
    expectRows("MATCH (n:`NORMALIZED`) RETURN count(n)", {{"0"}});
}

TEST_F(IsNormalizedPredicateTest, invalidFormIsASyntaxError) {
    expectError("RETURN 'a' IS 'NFC' NORMALIZED", "syntax error");
    expectError("RETURN 'a' IS NFX NORMALIZED", "syntax error");
    expectError("RETURN 'a' IS NFC NFD NORMALIZED", "The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED");
    expectError("RETURN 'a' IS NOT NOT NORMALIZED", "The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
