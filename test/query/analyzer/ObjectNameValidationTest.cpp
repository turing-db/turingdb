#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "AnalyzeException.h"
#include "TuringTest.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "ProcedureManager.h"
#include "SimpleGraph.h"
#include "versioning/Transaction.h"

using namespace db;

// A graph or an extension name becomes a file or a directory name. A backticked name can
// spell any character, so a path in it must still be turned away
class ObjectNameValidationTest : public turing::test::TuringTest {
public:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        _procedures = std::make_unique<ProcedureManager>();
        _procedures->init();
    }

protected:
    void analyzeQuery(const std::string& query) {
        CypherAST ast(_procedures.get(), query);

        CypherParser parser(&ast);
        parser.parse(query);

        const FrozenCommitTx transaction = _graph->openTransaction();
        CypherAnalyzer analyzer(&ast, transaction.viewGraph());

        analyzer.analyze();
    }

    void expectAccepted(const std::string& query) {
        EXPECT_NO_THROW(analyzeQuery(query)) << "query: " << query;
    }

    void expectRejected(const std::string& query, std::string_view reason) {
        try {
            analyzeQuery(query);
        } catch (const AnalyzeException& error) {
            const std::string message = error.what();
            EXPECT_NE(message.find(reason), std::string::npos)
                << "query: " << query << "\nerror: " << message;
            return;
        }

        ADD_FAILURE() << "query was accepted: " << query;
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ProcedureManager> _procedures;
};

TEST_F(ObjectNameValidationTest, createsAGraphNamedAfterAKeyword) {
    expectAccepted("CREATE GRAPH match");
}

TEST_F(ObjectNameValidationTest, rejectsACreatedGraphOutsideTheGraphsDirectory) {
    expectRejected("CREATE GRAPH `../x`", "character '.' not allowed");
}

TEST_F(ObjectNameValidationTest, rejectsACreatedGraphInASubdirectory) {
    expectRejected("CREATE GRAPH `a/b`", "character '/' not allowed");
}

TEST_F(ObjectNameValidationTest, rejectsACreatedGraphWithAnEmptyName) {
    expectRejected("CREATE GRAPH ``", "empty graph name");
}

TEST_F(ObjectNameValidationTest, loadsAGraphNamedAfterAKeyword) {
    expectAccepted("LOAD GRAPH `s3`");
}

TEST_F(ObjectNameValidationTest, rejectsALoadedGraphOutsideTheGraphsDirectory) {
    expectRejected("LOAD GRAPH `../x`", "character '.' not allowed");
}

TEST_F(ObjectNameValidationTest, rejectsALoadedGraphInASubdirectory) {
    expectRejected("LOAD GRAPH `a/b`", "character '/' not allowed");
}

TEST_F(ObjectNameValidationTest, rejectsALoadedGraphWithAnEmptyName) {
    expectRejected("LOAD GRAPH ``", "empty graph name");
}

TEST_F(ObjectNameValidationTest, rejectsAJsonlImportOutsideTheGraphsDirectory) {
    expectRejected("LOAD JSONL 'graph.jsonl' AS `../x`", "character '.' not allowed");
}

TEST_F(ObjectNameValidationTest, rejectsAGmlImportInASubdirectory) {
    expectRejected("LOAD GML 'graph.gml' AS `a/b`", "character '/' not allowed");
}

TEST_F(ObjectNameValidationTest, rejectsAParquetImportOutsideTheGraphsDirectory) {
    expectRejected("LOAD PARQUET 'graph.parquet' AS `../x`", "character '.' not allowed");
}

TEST_F(ObjectNameValidationTest, rejectsAnExtensionOutsideTheExtensionsDirectory) {
    expectRejected("INSTALL `../x`", "character '.' not allowed");
}

TEST_F(ObjectNameValidationTest, rejectsAnExtensionInASubdirectory) {
    expectRejected("INSTALL `a/b`", "character '/' not allowed");
}

TEST_F(ObjectNameValidationTest, rejectsAnExtensionWithAnEmptyName) {
    expectRejected("INSTALL ``", "extension name cannot be empty");
}
