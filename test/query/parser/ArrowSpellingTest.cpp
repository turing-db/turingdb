#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <string>
#include <string_view>

#include "CypherAST.h"
#include "CypherParser.h"
#include "EdgePattern.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "SinglePartQuery.h"
#include "WhereClause.h"
#include "expr/ExistsExpr.h"
#include "stmt/MatchStmt.h"
#include "stmt/StmtContainer.h"

using namespace db;

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
class ArrowSpellingTest : public ::testing::Test {
protected:
    const Pattern* parseMatchPattern(const std::string& query) {
        _query = query;
        _ast = std::make_unique<CypherAST>(nullptr, _query);

        CypherParser parser(_ast.get());
        parser.parse(_query);

        const SinglePartQuery* single = static_cast<const SinglePartQuery*>(_ast->queries().front());
        const MatchStmt* match = static_cast<const MatchStmt*>(single->getStmts()->stmts().front());

        return match->getPattern();
    }

    static void expectHop(const PatternElement* element, EdgePattern::Direction direction, const std::string& query) {
        const PatternElement::EntityPatterns& entities = element->getEntities();
        ASSERT_EQ(entities.size(), 3u) << query;

        const EdgePattern* edge = static_cast<const EdgePattern*>(entities[1]);
        EXPECT_EQ(edge->getDirection(), direction) << query;
    }

    void expectArrow(std::string_view arrow, EdgePattern::Direction direction) {
        std::string spelled;

        for (const std::string_view gap : GAPS) {
            spellWithGaps(arrow, gap, spelled);

            const std::string matched = "MATCH (a)" + spelled + "(b) RETURN a";
            const Pattern* pattern = parseMatchPattern(matched);
            expectHop(pattern->elements().front(), direction, matched);

            const std::string filtered = "MATCH (a), (b) WHERE (a)" + spelled + "(b) RETURN a";
            const Expr* predicate = parseMatchPattern(filtered)->getWhere()->getExpr();
            ASSERT_EQ(predicate->getKind(), Expr::Kind::EXISTS) << filtered;

            const Pattern* predicatePattern = static_cast<const ExistsExpr*>(predicate)->getPredicatePattern();
            expectHop(predicatePattern->elements().front(), direction, filtered);
        }
    }

    std::string _query;
    std::unique_ptr<CypherAST> _ast;
};

TEST_F(ArrowSpellingTest, readsTwoMinusSignsAsAnUndirectedRelationship) {
    expectArrow("--", EdgePattern::Direction::Undirected);
}

TEST_F(ArrowSpellingTest, readsAnArrowPointingRight) {
    expectArrow("-->", EdgePattern::Direction::Forward);
}

TEST_F(ArrowSpellingTest, readsAnArrowPointingLeft) {
    expectArrow("<--", EdgePattern::Direction::Backward);
}

TEST_F(ArrowSpellingTest, readsAnArrowPointingBothWays) {
    expectArrow("<-->", EdgePattern::Direction::Undirected);
}

TEST_F(ArrowSpellingTest, readsAnUndirectedRelationshipWithADetail) {
    expectArrow("-[e]-", EdgePattern::Direction::Undirected);
}

TEST_F(ArrowSpellingTest, readsARelationshipWithADetailPointingRight) {
    expectArrow("-[e]->", EdgePattern::Direction::Forward);
}

TEST_F(ArrowSpellingTest, readsARelationshipWithADetailPointingLeft) {
    expectArrow("<-[e]-", EdgePattern::Direction::Backward);
}

TEST_F(ArrowSpellingTest, readsARelationshipWithADetailPointingBothWays) {
    expectArrow("<-[e]->", EdgePattern::Direction::Undirected);
}
