#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>

#include "CypherAST.h"
#include "CypherParser.h"
#include "QualifiedName.h"
#include "Symbol.h"
#include "expr/Expr.h"
#include "expr/PropertyExpr.h"
#include "expr/PropertyLookupExpr.h"

using namespace db;

// Neo4j parses `m.a.b.c` as `((m.a).b).c`: a property of a variable, then a lookup of each
// further name in the value before it.
class DottedPropertyChainTest : public ::testing::Test {
protected:
    void parseQuery(std::string_view query) {
        _query = query;
        _ast = std::make_unique<CypherAST>(nullptr, _query);

        CypherParser parser(_ast.get());
        parser.parse(_query);
    }

    // The one property read no other read is looked up in, spelled with a pair of
    // parentheses around each value a name is looked up in
    void spellOutermostRead(std::string& spelling) const {
        std::unordered_set<const Expr*> bases;
        for (const Expr* expr : _ast->getExpressions()) {
            if (expr->getKind() == Expr::Kind::PROPERTY_LOOKUP) {
                bases.insert(static_cast<const PropertyLookupExpr*>(expr)->getBase());
            }
        }

        const Expr* outermost = nullptr;
        for (const Expr* expr : _ast->getExpressions()) {
            const Expr::Kind kind = expr->getKind();
            const bool readsAProperty = kind == Expr::Kind::PROPERTY || kind == Expr::Kind::PROPERTY_LOOKUP;

            if (readsAProperty && !bases.contains(expr)) {
                ASSERT_EQ(outermost, nullptr) << _query;
                outermost = expr;
            }
        }

        ASSERT_NE(outermost, nullptr) << _query;

        spell(outermost, spelling);
    }

    static void spell(const Expr* expr, std::string& spelling) {
        if (expr->getKind() == Expr::Kind::PROPERTY_LOOKUP) {
            const PropertyLookupExpr* lookup = static_cast<const PropertyLookupExpr*>(expr);

            spelling += '(';
            spell(lookup->getBase(), spelling);
            spelling += ").";
            spelling += lookup->getPropName();
        } else if (expr->getKind() == Expr::Kind::PROPERTY) {
            const PropertyExpr* property = static_cast<const PropertyExpr*>(expr);
            const QualifiedName* fullName = property->getFullName();

            std::string names;
            for (const Symbol* name : fullName->names()) {
                if (!names.empty()) {
                    names += '.';
                }

                names += name->getName();
            }

            spelling += names;
        } else {
            spelling += '?';
        }
    }

    std::string _query;
    std::unique_ptr<CypherAST> _ast;
};

TEST_F(DottedPropertyChainTest, aVariableAndOneNameAreAProperty) {
    ASSERT_NO_THROW(parseQuery("WITH {a: 1} AS m RETURN m.a"));

    std::string spelling;
    spellOutermostRead(spelling);

    EXPECT_EQ(spelling, "m.a");
}

TEST_F(DottedPropertyChainTest, aThirdNameIsLookedUpInTheProperty) {
    ASSERT_NO_THROW(parseQuery("MATCH (n:Person) RETURN n.name.year"));

    std::string spelling;
    spellOutermostRead(spelling);

    EXPECT_EQ(spelling, "(n.name).year");
}

TEST_F(DottedPropertyChainTest, eachFurtherNameIsLookedUpInTheValueBeforeIt) {
    ASSERT_NO_THROW(parseQuery("WITH {a: {b: {c: 1}}} AS m RETURN m.a.b.c"));

    std::string spelling;
    spellOutermostRead(spelling);

    EXPECT_EQ(spelling, "((m.a).b).c");
}

TEST_F(DottedPropertyChainTest, theDottedAndTheParenthesizedSpellingsBuildOneTree) {
    std::string dotted;
    ASSERT_NO_THROW(parseQuery("WITH {a: {b: {c: {d: 1}}}} AS m RETURN m.a.b.c.d"));
    spellOutermostRead(dotted);

    std::string parenthesized;
    ASSERT_NO_THROW(parseQuery("WITH {a: {b: {c: {d: 1}}}} AS m RETURN (((m.a).b).c).d"));
    spellOutermostRead(parenthesized);

    EXPECT_EQ(dotted, "(((m.a).b).c).d");
    EXPECT_EQ(dotted, parenthesized);
}

TEST_F(DottedPropertyChainTest, aChainAfterAParenthesizedVariableIsLookedUpToo) {
    ASSERT_NO_THROW(parseQuery("WITH {a: {b: 1}} AS m RETURN (m).a.b"));

    std::string spelling;
    spellOutermostRead(spelling);

    EXPECT_EQ(spelling, "(m.a).b");
}
