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

class ASTDumpEntityTest : public ::testing::Test {
protected:
    void SetUp() override {
        _procedures = std::make_unique<ProcedureManager>();
        _procedures->init();
    }

    void collectScriptEntities(std::string_view query, std::vector<std::string>& entities) {
        CypherAST ast(_procedures.get(), query);
        CypherParser parser(&ast);
        parser.parse(query);

        std::ostringstream dump;
        CypherASTDumper dumper(&ast);
        dumper.dump(dump);

        const std::string_view prefix = "    script ||--o{ ";
        const std::string_view suffix = " : \"\"";

        std::istringstream lines(dump.str());
        std::string line;
        while (std::getline(lines, line)) {
            if (line.starts_with(prefix) && line.ends_with(suffix)) {
                entities.push_back(line.substr(prefix.size(), line.size() - prefix.size() - suffix.size()));
            }
        }
    }

    void expectOneEntity(std::string_view query, std::string_view entity) {
        std::vector<std::string> entities;
        collectScriptEntities(query, entities);

        EXPECT_EQ(entities, (std::vector<std::string> {std::string(entity)})) << "query: " << query;
    }

    void expectOneEntity(std::string_view query) {
        std::vector<std::string> entities;
        collectScriptEntities(query, entities);

        EXPECT_EQ(entities.size(), 1u) << "query: " << query;
    }

    std::unique_ptr<ProcedureManager> _procedures;
};

TEST_F(ASTDumpEntityTest, showVectorIndexesDumpsOnlyItsOwnEntity) {
    expectOneEntity("SHOW VECTOR INDEXES", "SHOW_VECTOR_INDEXES");
}

TEST_F(ASTDumpEntityTest, everyNamedSystemStatementDumpsOnlyItsOwnEntity) {
    expectOneEntity("SHOW PROCEDURES", "SHOW_PROCEDURES");
    expectOneEntity("CREATE VECTOR INDEX v WITH DIMENSION 4 METRIC EUCLID", "CREATE_VECTOR_INDEX");
    expectOneEntity("LOAD VECTOR FROM 'f' IN v", "LOAD_VECTOR");
    expectOneEntity("LOAD EMBEDDING FROM 'f' AS e", "LOAD_EMBEDDING");
    expectOneEntity("DELETE VECTOR INDEX v", "DELETE_VECTOR_INDEX");
    expectOneEntity("LOAD COMMIT 'h'", "LOAD_COMMIT");
    expectOneEntity("INSTALL x", "INSTALL_EXTENSION");
    expectOneEntity("SHOW EXTENSIONS", "SHOW_EXTENSIONS");
    expectOneEntity("CREATE INDEX i FOR (n:P) ON n.x", "CREATE_NODE_PROPERTY_INDEX");
    expectOneEntity("CREATE INDEX i FOR [e:T] ON e.x", "CREATE_EDGE_PROPERTY_INDEX");
    expectOneEntity("DROP INDEX i", "DROP_INDEX");
}

TEST_F(ASTDumpEntityTest, everyOtherStatementDumpsOneEntity) {
    expectOneEntity("MATCH (n) RETURN n");
    expectOneEntity("MATCH (n) RETURN n.x AS x UNION RETURN 2 AS x");
    expectOneEntity("WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS x");
    expectOneEntity("LOAD GRAPH g");
    expectOneEntity("LOAD JSONL 'f'");
    expectOneEntity("CHANGE NEW");
    expectOneEntity("COMMIT");
    expectOneEntity("LIST GRAPH");
    expectOneEntity("LIST AVAILABLE GRAPHS");
    expectOneEntity("CREATE GRAPH g");
    expectOneEntity("LOAD GML 'f'");
    expectOneEntity("LOAD PARQUET 'f' AS g");
    expectOneEntity("S3 CONNECT 'a' 'b' 'c'");
    expectOneEntity("S3 PULL 'a' 'b'");
    expectOneEntity("MERGE_DATAPARTS");
}
