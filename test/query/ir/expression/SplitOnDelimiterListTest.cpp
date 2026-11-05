#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class SplitOnDelimiterListTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.delimiters = [',', ';'], p.held = [[',', ';'], 1]");
    }
};

TEST_F(SplitOnDelimiterListTest, splitsOnEveryDelimiterOfTheList) {
    expectRows("RETURN split('a,b;c', [',', ';'])", {{"[a, b, c]"}});
}

TEST_F(SplitOnDelimiterListTest, keepsEmptyParts) {
    expectRows("RETURN split(',a;;b,', [',', ';'])", {{"[, a, , b, ]"}});
}

TEST_F(SplitOnDelimiterListTest, cutsAtTheEarliestDelimiterWhateverItsPlaceInTheList) {
    expectRows("RETURN split('a;b,c', [',', ';']), split('a<>b>c', ['>', '<>'])", {{"[a, b, c]", "[a, b, c]"}});
}

TEST_F(SplitOnDelimiterListTest, cutsAtTheFirstOfTheDelimitersStartingAtOnePlace) {
    expectRows("RETURN split('a--b', ['-', '--']), split('a--b', ['--', '-'])", {{"[a, , b]", "[a, b]"}});
}

TEST_F(SplitOnDelimiterListTest, cutsOnMultiByteDelimiters) {
    expectRows("RETURN split('aébèc', ['é', 'è'])", {{"[a, b, c]"}});
}

TEST_F(SplitOnDelimiterListTest, listOfOneDelimiterSplitsAsTheDelimiter) {
    expectRows("RETURN split('a,b', [',']), split('a,,b', [','])", {{"[a, b]", "[a, , b]"}});
}

TEST_F(SplitOnDelimiterListTest, emptyListLeavesTheStringWhole) {
    expectRows("RETURN split('a,b', [])", {{"[a,b]"}});
}

TEST_F(SplitOnDelimiterListTest, emptyStringIsOneEmptyPart) {
    expectRows("RETURN size(split('', [',', ';'])), split('', [',', ';'])[0] = ''", {{"1", "true"}});
}

TEST_F(SplitOnDelimiterListTest, delimiterEqualToTheStringLeavesTwoEmptyParts) {
    expectRows("RETURN split('abc', ['a', 'abc'])", {{"[, ]"}});
}

TEST_F(SplitOnDelimiterListTest, emptyDelimiterSplitsWhatTheOthersLeaveIntoCharacters) {
    expectRows("RETURN split('a,b;c', [',', '']), split('é,t', ['', ','])", {{"[a, b, ;, c]", "[é, t]"}});
}

TEST_F(SplitOnDelimiterListTest, nullInTheListIsNull) {
    expectRows("RETURN split('a,b', [',', null])", {{"null"}});
}

TEST_F(SplitOnDelimiterListTest, nullStringIsNull) {
    expectRows("RETURN split(null, [',', ';'])", {{"null"}});
}

TEST_F(SplitOnDelimiterListTest, readsADelimiterListPerRow) {
    expectRows("UNWIND [[','], [';'], [',', ';'], null, [',', null]] AS d RETURN split('a,b;c', d)",
               {{"[a, b;c]"}, {"[a,b, c]"}, {"[a, b, c]"}, {"null"}, {"null"}});
}

TEST_F(SplitOnDelimiterListTest, buildsTheDelimiterListPerRow) {
    expectRows("UNWIND [',', ';'] AS d RETURN split('a,b;c', [d, '-'])", {{"[a, b;c]"}, {"[a,b, c]"}});
}

TEST_F(SplitOnDelimiterListTest, splitsAPropertyPerRow) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Maxime', 'Remy'] RETURN p.name, split(p.name, ['a', 'e'])",
               {{"Maxime", "[M, xim, ]"}, {"Remy", "[R, my]"}});
}

TEST_F(SplitOnDelimiterListTest, partsAreStrings) {
    expectRows("UNWIND split('x;y,z', [',', ';']) AS w RETURN toUpper(w)", {{"X"}, {"Y"}, {"Z"}});
}

TEST_F(SplitOnDelimiterListTest, readsADelimiterListProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN split('a,b;c', p.delimiters)", {{"[a, b, c]"}});
}

TEST_F(SplitOnDelimiterListTest, readsADelimiterListATaggedCellHolds) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN split('a,b;c', p.held[0])", {{"[a, b, c]"}});
}

TEST_F(SplitOnDelimiterListTest, rejectsADelimiterThatIsNoString) {
    expectError("RETURN split('a,b', [',', 1])", "split()");
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN split('a,b', p.held[1])", "split()");
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN split('a,b', p.held)", "split()");
}

TEST_F(SplitOnDelimiterListTest, rejectsADelimiterThatIsNoStringOnlyWhereItIsEvaluated) {
    expectRows("UNWIND [] AS x RETURN split('a,b', [',', 1])", {});
    expectRows("MATCH (p:Person {name: 'Nobody'}) RETURN split('a,b', [',', 1])", {});
}
