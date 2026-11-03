#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>

#include "ExpectedASTTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::array<std::string_view, 5> GAPS {"", " ", "\n", "// c\n", "/* c */"};

void spellWithGaps(std::string_view arrow, std::string_view gap, std::string& spelled) {
    spelled.clear();

    for (const char character : arrow) {
        if (!spelled.empty()) {
            spelled += gap;
        }

        spelled += character;
    }
}

}

// An arrow is read from its characters wherever a relationship may stand, so whitespace and
// comments may separate them as they separate any two tokens
class ArrowSpellingTest : public ExpectedASTTest {
protected:
    void expectArrow(std::string_view arrow, EdgePattern::Direction direction, std::string_view edgeSymbol) {
        std::string spelled;

        for (const std::string_view gap : GAPS) {
            spellWithGaps(arrow, gap, spelled);

            expectParse("MATCH (a)" + spelled + "(b) RETURN a",
                        {query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                                  edge({._direction = direction, ._symbol = edgeSymbol}),
                                                                  node({._symbol = "b"})})})})},
                               returning({symbol("a")}))});

            expectParse("MATCH (a), (b) WHERE (a)" + spelled + "(b) RETURN a",
                        {query({match({._pattern = pattern({path({node({._symbol = "a"})}), path({node({._symbol = "b"})})},
                                                           where(patternPredicate(pattern({path({node({._symbol = "a"}),
                                                                                                 edge({._direction = direction,
                                                                                                       ._symbol = edgeSymbol}),
                                                                                                 node({._symbol = "b"})})}))))})},
                               returning({symbol("a")}))});
        }
    }
};

TEST_F(ArrowSpellingTest, readsTwoMinusSignsAsAnUndirectedRelationship) {
    expectArrow("--", EdgePattern::Direction::Undirected, "");
}

TEST_F(ArrowSpellingTest, readsAnArrowPointingRight) {
    expectArrow("-->", EdgePattern::Direction::Forward, "");
}

TEST_F(ArrowSpellingTest, readsAnArrowPointingLeft) {
    expectArrow("<--", EdgePattern::Direction::Backward, "");
}

TEST_F(ArrowSpellingTest, readsAnArrowPointingBothWays) {
    expectArrow("<-->", EdgePattern::Direction::Undirected, "");
}

TEST_F(ArrowSpellingTest, readsAnUndirectedRelationshipWithADetail) {
    expectArrow("-[e]-", EdgePattern::Direction::Undirected, "e");
}

TEST_F(ArrowSpellingTest, readsARelationshipWithADetailPointingRight) {
    expectArrow("-[e]->", EdgePattern::Direction::Forward, "e");
}

TEST_F(ArrowSpellingTest, readsARelationshipWithADetailPointingLeft) {
    expectArrow("<-[e]-", EdgePattern::Direction::Backward, "e");
}

TEST_F(ArrowSpellingTest, readsARelationshipWithADetailPointingBothWays) {
    expectArrow("<-[e]->", EdgePattern::Direction::Undirected, "e");
}
