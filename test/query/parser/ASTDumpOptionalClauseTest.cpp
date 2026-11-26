#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "CypherAST.h"
#include "CypherASTDumper.h"
#include "CypherParser.h"
#include "ProcedureManager.h"

using namespace db;

class ASTDumpOptionalClauseTest : public ::testing::Test {
protected:
    void SetUp() override {
        _procedures = std::make_unique<ProcedureManager>();
        _procedures->init();
    }

    void collectClauseTypes(std::string_view query, std::vector<std::string>& clauseTypes) {
        CypherAST ast(_procedures.get(), query);
        CypherParser parser(&ast);
        parser.parse(query);

        std::ostringstream dump;
        CypherASTDumper dumper(&ast);
        dumper.dump(dump);

        const std::string_view prefix = "        ASTType ";

        std::istringstream lines(dump.str());
        std::string line;
        while (std::getline(lines, line)) {
            if (!line.starts_with(prefix)) {
                continue;
            }

            const std::string type = line.substr(prefix.size());
            const bool isMatch = type.find("MATCH") != std::string::npos;
            const bool isCall = type.find("CALL") != std::string::npos;
            if (isMatch || isCall) {
                clauseTypes.push_back(type);
            }
        }
    }

    void expectClauseTypes(std::string_view query, const std::vector<std::string>& expected) {
        std::vector<std::string> clauseTypes;
        collectClauseTypes(query, clauseTypes);

        EXPECT_EQ(clauseTypes, expected) << "query: " << query;
    }

    std::unique_ptr<ProcedureManager> _procedures;
};

TEST_F(ASTDumpOptionalClauseTest, plainProcedureCallDumpsAsCall) {
    expectClauseTypes("CALL db.labels()", {"CALL"});
    expectClauseTypes("CALL db.labels() YIELD label RETURN label", {"CALL"});
}

TEST_F(ASTDumpOptionalClauseTest, optionalProcedureCallDumpsAsOptionalCall) {
    expectClauseTypes("OPTIONAL CALL db.labels()", {"OPTIONAL_CALL"});
    expectClauseTypes("MATCH (n) OPTIONAL CALL db.labels() YIELD label RETURN n, label",
                      {"MATCH", "OPTIONAL_CALL"});
}

TEST_F(ASTDumpOptionalClauseTest, optionalCallSubqueryDumpsAsOptionalCallSubquery) {
    expectClauseTypes("MATCH (n) OPTIONAL CALL (n) { MATCH (n)-->(m) RETURN m } RETURN n, m",
                      {"MATCH", "OPTIONAL_CALL_SUBQUERY", "MATCH"});
}

TEST_F(ASTDumpOptionalClauseTest, plainCallSubqueryDumpsAsCallSubquery) {
    expectClauseTypes("MATCH (n) CALL (n) { MATCH (n)-->(m) RETURN m } RETURN n, m",
                      {"MATCH", "CALL_SUBQUERY", "MATCH"});
}

TEST_F(ASTDumpOptionalClauseTest, matchKeepsItsOptionalLabel) {
    expectClauseTypes("MATCH (n) RETURN n", {"MATCH"});
    expectClauseTypes("OPTIONAL MATCH (n) RETURN n", {"OPTIONAL_MATCH"});
}

TEST_F(ASTDumpOptionalClauseTest, everyClauseKeepsItsOwnOptionality) {
    expectClauseTypes("MATCH (a) "
                      "OPTIONAL MATCH (a)-->(b) "
                      "CALL db.labels() YIELD label "
                      "OPTIONAL CALL db.edgeTypes() YIELD edgeType "
                      "CALL (a) { MATCH (a)-->(c) RETURN c } "
                      "OPTIONAL CALL (a) { OPTIONAL MATCH (a)<--(d) RETURN d } "
                      "RETURN a, b, label, edgeType, c, d",
                      {"MATCH",
                       "OPTIONAL_MATCH",
                       "CALL",
                       "OPTIONAL_CALL",
                       "CALL_SUBQUERY",
                       "MATCH",
                       "OPTIONAL_CALL_SUBQUERY",
                       "OPTIONAL_MATCH"});
}
