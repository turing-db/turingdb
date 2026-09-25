#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "CypherAST.h"
#include "CypherParser.h"
#include "ParserException.h"
#include "QualifiedName.h"
#include "Symbol.h"
#include "expr/Expr.h"
#include "expr/PropertyExpr.h"

using namespace db;

// openCypher lets a property access parenthesize the variable it reads: `(p).age` is
// `p.age`, and Neo4j 5.26 accepts it wherever a property access is accepted. The parser
// must build the same PropertyExpr for both spellings, so that nothing downstream can
// tell them apart.
class ParenthesizedPropertyAccessTest : public ::testing::Test {
protected:
    void parseQuery(std::string_view query) {
        _query = query;
        _ast = std::make_unique<CypherAST>(nullptr, _query);

        CypherParser parser(_ast.get());
        parser.parse(_query);
    }

    // Every property access the parse produced, spelled `variable.property`, in the order
    // the parser created them
    void collectPropertyAccesses(std::vector<std::string>& accesses) const {
        for (const Expr* expr : _ast->getExpressions()) {
            if (expr->getKind() != Expr::Kind::PROPERTY) {
                continue;
            }

            const PropertyExpr* property = static_cast<const PropertyExpr*>(expr);
            const QualifiedName* fullName = property->getFullName();

            std::string& access = accesses.emplace_back();
            for (const Symbol* name : fullName->names()) {
                if (!access.empty()) {
                    access += '.';
                }

                access += name->getName();
            }
        }
    }

    std::string _query;
    std::unique_ptr<CypherAST> _ast;
};

TEST_F(ParenthesizedPropertyAccessTest, returnReadsTheParenthesizedVariable) {
    ASSERT_NO_THROW(parseQuery("MATCH (p:Person {name: 'Remy'}) RETURN (p).age"));

    std::vector<std::string> accesses;
    collectPropertyAccesses(accesses);

    ASSERT_EQ(accesses.size(), 1u);
    EXPECT_EQ(accesses[0], "p.age");
}

TEST_F(ParenthesizedPropertyAccessTest, whereReadsTheParenthesizedVariable) {
    ASSERT_NO_THROW(parseQuery("MATCH (p:Person {name: 'Remy'}) WHERE (p).age = 32 RETURN p.name"));

    std::vector<std::string> accesses;
    collectPropertyAccesses(accesses);

    ASSERT_EQ(accesses.size(), 2u);
    EXPECT_EQ(accesses[0], "p.age");
    EXPECT_EQ(accesses[1], "p.name");
}

TEST_F(ParenthesizedPropertyAccessTest, setWritesTheParenthesizedVariable) {
    ASSERT_NO_THROW(parseQuery("MATCH (p:Person {name: 'Remy'}) SET (p).age = 1 RETURN p.age"));

    std::vector<std::string> accesses;
    collectPropertyAccesses(accesses);

    ASSERT_EQ(accesses.size(), 2u);
    EXPECT_EQ(accesses[0], "p.age");
    EXPECT_EQ(accesses[1], "p.age");
}

TEST_F(ParenthesizedPropertyAccessTest, removeDropsTheParenthesizedVariable) {
    ASSERT_NO_THROW(parseQuery("MATCH (p:Person {name: 'Remy'}) REMOVE (p).age RETURN p.age"));

    std::vector<std::string> accesses;
    collectPropertyAccesses(accesses);

    ASSERT_EQ(accesses.size(), 2u);
    EXPECT_EQ(accesses[0], "p.age");
    EXPECT_EQ(accesses[1], "p.age");
}

// The inner pair already unwrapped the variable, so the outer one has a plain variable to
// unwrap in turn. Any depth of nesting reaches the same access
TEST_F(ParenthesizedPropertyAccessTest, nestedParenthesesReachTheSameAccess) {
    ASSERT_NO_THROW(parseQuery("MATCH (p:Person {name: 'Remy'}) SET ((p)).age = 2 RETURN p.age"));

    std::vector<std::string> accesses;
    collectPropertyAccesses(accesses);

    ASSERT_EQ(accesses.size(), 2u);
    EXPECT_EQ(accesses[0], "p.age");
    EXPECT_EQ(accesses[1], "p.age");
}

TEST_F(ParenthesizedPropertyAccessTest, theTwoSpellingsBuildTheSameName) {
    ASSERT_NO_THROW(parseQuery("MATCH (p) RETURN (p).age, p.age"));

    std::vector<std::string> accesses;
    collectPropertyAccesses(accesses);

    ASSERT_EQ(accesses.size(), 2u);
    EXPECT_EQ(accesses[0], accesses[1]);
    EXPECT_EQ(accesses[0], "p.age");
}

TEST_F(ParenthesizedPropertyAccessTest, anEdgeVariableParenthesizesToo) {
    ASSERT_NO_THROW(parseQuery("MATCH (p)-[r]->(q) RETURN (r).since"));

    std::vector<std::string> accesses;
    collectPropertyAccesses(accesses);

    ASSERT_EQ(accesses.size(), 1u);
    EXPECT_EQ(accesses[0], "r.since");
}

// The access is an expression like any other, so it reaches every place one is read
TEST_F(ParenthesizedPropertyAccessTest, theAccessIsAnExpressionLikeAnyOther) {
    EXPECT_NO_THROW(parseQuery("MATCH (p) RETURN (p).age + 1"));
    EXPECT_NO_THROW(parseQuery("MATCH (p) RETURN count((p).age)"));
    EXPECT_NO_THROW(parseQuery("MATCH (p) WITH (p).age AS a RETURN a"));
    EXPECT_NO_THROW(parseQuery("MATCH (p) RETURN (p).age AS a ORDER BY (p).name"));
}

// Only a variable can be parenthesized. PropertyExpr roots at a name, so a value on the
// left of the dot has no name to root at and the query is turned away at the parse
TEST_F(ParenthesizedPropertyAccessTest, aValueOnTheLeftOfTheDotIsRejected) {
    EXPECT_THROW(parseQuery("MATCH (p) RETURN (1 + 2).foo"), ParserException);
    EXPECT_THROW(parseQuery("MATCH (p) RETURN ({a: 1}).a"), ParserException);
    EXPECT_THROW(parseQuery("MATCH (p) RETURN (p.age).foo"), ParserException);
}

// The rule reads `(` as the start of a parenthesized expression only where an expression
// is what follows. A pattern still opens with the same token
TEST_F(ParenthesizedPropertyAccessTest, patternsStillParse) {
    EXPECT_NO_THROW(parseQuery("MATCH (a)--(b) RETURN a"));
    EXPECT_NO_THROW(parseQuery("MATCH (p) WHERE (p)-->() RETURN p"));
    EXPECT_NO_THROW(parseQuery("MATCH (p) RETURN (p)['age']"));
}
