#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr const char* COMPOSED_E_ACUTE = "é";
constexpr const char* DECOMPOSED_E_ACUTE = "é";
constexpr const char* FI_LIGATURE = "ﬁ";
constexpr const char* COMPOSED_E_DOT_CIRCUMFLEX = "ệ";
constexpr const char* DECOMPOSED_E_DOT_CIRCUMFLEX = "ệ";
constexpr const char* HANGUL_GA = "가";
constexpr const char* HANGUL_GA_JAMO = "가";

std::string quoted(std::string_view text) {
    return "'" + std::string(text) + "'";
}

}

class NormalizeFunctionTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.nick = 'Rémy', p.words = ['é', 3, null]");
        applyWrite("MATCH (p:Person {name: 'Adam'}) SET p.nick = 'ﬁne'");
    }
};

TEST_F(NormalizeFunctionTest, defaultFormIsNFC) {
    expectRows("RETURN normalize(" + quoted(DECOMPOSED_E_ACUTE) + "), normalize(" + quoted(COMPOSED_E_ACUTE) + ")",
               {{COMPOSED_E_ACUTE, COMPOSED_E_ACUTE}});
}

TEST_F(NormalizeFunctionTest, nfcComposes) {
    expectRows("RETURN normalize(" + quoted(DECOMPOSED_E_ACUTE) + ", NFC), normalize(" + quoted(COMPOSED_E_ACUTE) + ", NFC), "
               "normalize(" + quoted(HANGUL_GA_JAMO) + ", NFC), normalize(" + quoted(FI_LIGATURE) + ", NFC)",
               {{COMPOSED_E_ACUTE, COMPOSED_E_ACUTE, HANGUL_GA, FI_LIGATURE}});
}

TEST_F(NormalizeFunctionTest, nfdDecomposes) {
    expectRows("RETURN normalize(" + quoted(COMPOSED_E_ACUTE) + ", NFD), normalize(" + quoted(DECOMPOSED_E_ACUTE) + ", NFD), "
               "normalize(" + quoted(HANGUL_GA) + ", NFD), normalize(" + quoted(FI_LIGATURE) + ", NFD)",
               {{DECOMPOSED_E_ACUTE, DECOMPOSED_E_ACUTE, HANGUL_GA_JAMO, FI_LIGATURE}});
}

TEST_F(NormalizeFunctionTest, nfkcComposesAndFoldsCompatibilityCharacters) {
    expectRows("RETURN normalize(" + quoted(DECOMPOSED_E_ACUTE) + ", NFKC), normalize(" + quoted(COMPOSED_E_ACUTE) + ", NFKC), "
               "normalize(" + quoted(FI_LIGATURE) + ", NFKC)",
               {{COMPOSED_E_ACUTE, COMPOSED_E_ACUTE, "fi"}});
}

TEST_F(NormalizeFunctionTest, nfkdDecomposesAndFoldsCompatibilityCharacters) {
    expectRows("RETURN normalize(" + quoted(COMPOSED_E_ACUTE) + ", NFKD), normalize(" + quoted(DECOMPOSED_E_ACUTE) + ", NFKD), "
               "normalize(" + quoted(FI_LIGATURE) + ", NFKD)",
               {{DECOMPOSED_E_ACUTE, DECOMPOSED_E_ACUTE, "fi"}});
}

TEST_F(NormalizeFunctionTest, combiningMarksAreReorderedCanonically) {
    expectRows("RETURN normalize('ệ'), normalize(" + quoted(COMPOSED_E_DOT_CIRCUMFLEX) + ", NFD)",
               {{COMPOSED_E_DOT_CIRCUMFLEX, DECOMPOSED_E_DOT_CIRCUMFLEX}});
}

TEST_F(NormalizeFunctionTest, asciiIsUnchanged) {
    expectRows("RETURN normalize('hello'), normalize('hello', NFD), normalize('', NFKC), normalize('a b', NFKD)",
               {{"hello", "hello", "", "a b"}});
}

TEST_F(NormalizeFunctionTest, formKeywordIsCaseInsensitive) {
    expectRows("RETURN NORMALIZE(" + quoted(COMPOSED_E_ACUTE) + ", nfd), Normalize(" + quoted(FI_LIGATURE) + ", Nfkc)",
               {{DECOMPOSED_E_ACUTE, "fi"}});
}

TEST_F(NormalizeFunctionTest, normalizedFormsCompareEqual) {
    expectRows("RETURN normalize(" + quoted(DECOMPOSED_E_ACUTE) + ") = " + quoted(COMPOSED_E_ACUTE) + ", "
               + quoted(DECOMPOSED_E_ACUTE) + " = " + quoted(COMPOSED_E_ACUTE),
               {{"true", "false"}});
}

TEST_F(NormalizeFunctionTest, normalizeOfNullIsNull) {
    expectRows("RETURN normalize(null), normalize(null, NFD), normalize(null, NFKC)", {{"null", "null", "null"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN normalize(p.nosuch), normalize(p.nosuch, NFKD)", {{"null", "null"}});
    expectRows("UNWIND [" + quoted(DECOMPOSED_E_ACUTE) + ", null] AS s RETURN normalize(s)", {{COMPOSED_E_ACUTE}, {"null"}});
}

TEST_F(NormalizeFunctionTest, normalizeReadsAPropertyPerRow) {
    expectRows("MATCH (p:Person) WHERE p.nick IS NOT NULL RETURN p.name, normalize(p.nick), normalize(p.nick, NFKC)",
               {{"Remy", "Rémy", "Rémy"}, {"Adam", "ﬁne", "fine"}});
    expectRows("MATCH (p:Person) WHERE normalize(p.nick, NFKD) = 'fine' RETURN p.name", {{"Adam"}});
}

TEST_F(NormalizeFunctionTest, normalizeReadsAStringPerRow) {
    expectRows("UNWIND [" + quoted(COMPOSED_E_ACUTE) + ", " + quoted(DECOMPOSED_E_ACUTE) + ", " + quoted(FI_LIGATURE) + "] AS s "
               "RETURN normalize(s, NFD), normalize(s, NFKC)",
               {{DECOMPOSED_E_ACUTE, COMPOSED_E_ACUTE}, {DECOMPOSED_E_ACUTE, COMPOSED_E_ACUTE}, {FI_LIGATURE, "fi"}});
}

TEST_F(NormalizeFunctionTest, normalizedStringsGroupTogether) {
    expectRows("UNWIND [" + quoted(COMPOSED_E_ACUTE) + ", " + quoted(DECOMPOSED_E_ACUTE) + "] AS s RETURN normalize(s), count(*)",
               {{COMPOSED_E_ACUTE, "2"}});
}

TEST_F(NormalizeFunctionTest, constantIsLaidOutOverEveryRow) {
    expectRows("MATCH (p:Person) RETURN normalize(" + quoted(FI_LIGATURE) + ", NFKD), count(p)", {{"fi", "8"}});
}

TEST_F(NormalizeFunctionTest, normalizeReadsTaggedCells) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN normalize(p.words[0]), normalize(p.words[0], NFD), normalize(p.words[2], NFKC)",
               {{COMPOSED_E_ACUTE, DECOMPOSED_E_ACUTE, "null"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) UNWIND [p.words[0], p.words[2]] AS w RETURN normalize(w, NFD)",
               {{DECOMPOSED_E_ACUTE}, {"null"}});
}

TEST_F(NormalizeFunctionTest, normalizeRaisesOnACellHoldingNoString) {
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN normalize(p.words[1])", "normalize() reads a string, not 'Integer'");
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN normalize(p.words[1], NFKD)", "normalize() reads a string, not 'Integer'");
    expectError("MATCH (p:Person {name: 'Remy'}) UNWIND p.words AS w RETURN normalize(w, NFD)",
                "normalize() reads a string, not 'Integer'");
}

TEST_F(NormalizeFunctionTest, formNamesRemainUsableAsNames) {
    expectRows("WITH 'x' AS nfc, 'y' AS nfkd, 'z' AS normalize RETURN normalize(nfc, NFD), nfkd, normalize", {{"x", "y", "z"}});
    expectRows("WITH {normalize: 1, NFC: 2} AS m RETURN m.normalize, m.NFC", {{"1", "2"}});
}

TEST_F(NormalizeFunctionTest, invalidFormIsRejected) {
    expectError("RETURN normalize('a', XYZ)", "expecting NFC or NFD or NFKC or NFKD");
    expectError("RETURN normalize('a', 'NFC')", "expecting NFC or NFD or NFKC or NFKD");
    expectError("WITH 'NFC' AS form RETURN normalize('a', form)", "expecting NFC or NFD or NFKC or NFKD");
    expectError("RETURN normalize('a', NFC, NFD)", "syntax error");
    expectError("RETURN normalize()", "syntax error");
}

TEST_F(NormalizeFunctionTest, nonStringIsNull) {
    expectRows("RETURN normalize(1), normalize(1, NFD), normalize([1], NFC), normalize(true), normalize(1.5, NFKC), "
               "normalize({a: 'b'}, NFKD)",
               {{"null", "null", "null", "null", "null", "null"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN normalize(p), normalize(p, NFD)", {{"null", "null"}});
    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name, normalize(p.age), normalize(p.age, NFKC)",
               {{"Remy", "null", "null"}, {"Adam", "null", "null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
