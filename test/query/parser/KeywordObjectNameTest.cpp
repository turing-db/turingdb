#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "CreateEdgePropertyIndexQuery.h"
#include "CreateGraphQuery.h"
#include "CreateNodePropertyIndexQuery.h"
#include "CreateVectorIndexQuery.h"
#include "CypherAST.h"
#include "CypherParser.h"
#include "DeleteVectorIndexQuery.h"
#include "DropIndexQuery.h"
#include "InstallExtensionQuery.h"
#include "LoadEmbeddingQuery.h"
#include "LoadGMLQuery.h"
#include "LoadGraphQuery.h"
#include "LoadJsonlQuery.h"
#include "LoadParquetQuery.h"
#include "LoadVectorQuery.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "stmt/StmtContainer.h"
#include "stmt/VectorSearchStmt.h"

using namespace db;

// A graph, an index, an extension and a property an import reads are named the way a
// variable is: a keyword or a backticked name is a name there too
class KeywordObjectNameTest : public ::testing::Test {
protected:
    const QueryCommand* parseCommand(std::string_view query, QueryCommand::Kind kind) {
        _query = query;
        _ast = std::make_unique<CypherAST>(nullptr, _query);

        CypherParser parser(_ast.get());
        parser.parse(_query);

        const QueryCommand* command = _ast->queries().front();
        EXPECT_EQ(command->getKind(), kind) << query;

        return command;
    }

    std::string _query;
    std::unique_ptr<CypherAST> _ast;
};

TEST_F(KeywordObjectNameTest, loadsAGraphS3) {
    const QueryCommand* command = parseCommand("LOAD GRAPH s3", QueryCommand::Kind::LOAD_GRAPH_QUERY);
    EXPECT_EQ(static_cast<const LoadGraphQuery*>(command)->getGraphName(), "s3");
}

TEST_F(KeywordObjectNameTest, loadsABacktickedGraph) {
    const QueryCommand* command = parseCommand("LOAD GRAPH `s3`", QueryCommand::Kind::LOAD_GRAPH_QUERY);
    EXPECT_EQ(static_cast<const LoadGraphQuery*>(command)->getGraphName(), "s3");
}

TEST_F(KeywordObjectNameTest, createsAGraphVector) {
    const QueryCommand* command = parseCommand("CREATE GRAPH vector", QueryCommand::Kind::CREATE_GRAPH_QUERY);
    EXPECT_EQ(static_cast<const CreateGraphQuery*>(command)->getGraphName(), "vector");
}

TEST_F(KeywordObjectNameTest, createsAGraphMatch) {
    const QueryCommand* command = parseCommand("CREATE GRAPH match", QueryCommand::Kind::CREATE_GRAPH_QUERY);
    EXPECT_EQ(static_cast<const CreateGraphQuery*>(command)->getGraphName(), "match");
}

TEST_F(KeywordObjectNameTest, createsABacktickedGraph) {
    const QueryCommand* command = parseCommand("CREATE GRAPH `my-graph`", QueryCommand::Kind::CREATE_GRAPH_QUERY);
    EXPECT_EQ(static_cast<const CreateGraphQuery*>(command)->getGraphName(), "my-graph");
}

TEST_F(KeywordObjectNameTest, createsANodeIndexGraph) {
    const QueryCommand* command = parseCommand("CREATE INDEX graph FOR (n:Person) ON n.name", QueryCommand::Kind::CREATE_NODE_PROPERTY_INDEX_QUERY);
    EXPECT_EQ(static_cast<const CreateNodePropertyIndexQuery*>(command)->indexName(), "graph");
}

TEST_F(KeywordObjectNameTest, createsAnEdgeIndexReturn) {
    const QueryCommand* command = parseCommand("CREATE INDEX return FOR [e:KNOWS_WELL] ON e.since", QueryCommand::Kind::CREATE_EDGE_PROPERTY_INDEX_QUERY);
    EXPECT_EQ(static_cast<const CreateEdgePropertyIndexQuery*>(command)->indexName(), "return");
}

TEST_F(KeywordObjectNameTest, dropsAnIndexGraph) {
    const QueryCommand* command = parseCommand("DROP INDEX graph", QueryCommand::Kind::DROP_INDEX_QUERY);
    EXPECT_EQ(static_cast<const DropIndexQuery*>(command)->indexName(), "graph");
}

TEST_F(KeywordObjectNameTest, dropsABacktickedIndex) {
    const QueryCommand* command = parseCommand("DROP INDEX `by name`", QueryCommand::Kind::DROP_INDEX_QUERY);
    EXPECT_EQ(static_cast<const DropIndexQuery*>(command)->indexName(), "by name");
}

TEST_F(KeywordObjectNameTest, createsAVectorIndexS3) {
    const QueryCommand* command = parseCommand("CREATE VECTOR INDEX s3 WITH DIMENSION 4 METRIC EUCLID", QueryCommand::Kind::CREATE_VECTOR_INDEX_QUERY);
    EXPECT_EQ(static_cast<const CreateVectorIndexQuery*>(command)->getIndexName(), "s3");
}

TEST_F(KeywordObjectNameTest, deletesAVectorIndexS3) {
    const QueryCommand* command = parseCommand("DELETE VECTOR INDEX s3", QueryCommand::Kind::DELETE_VECTOR_INDEX_QUERY);
    EXPECT_EQ(static_cast<const DeleteVectorIndexQuery*>(command)->getIndexName(), "s3");
}

TEST_F(KeywordObjectNameTest, loadsVectorsIntoAnIndexWhere) {
    const QueryCommand* command = parseCommand("LOAD VECTOR FROM 'vectors.csv' IN where", QueryCommand::Kind::LOAD_VECTOR_QUERY);
    EXPECT_EQ(static_cast<const LoadVectorQuery*>(command)->getIndexName(), "where");
}

TEST_F(KeywordObjectNameTest, loadsEmbeddingsAsAPropertyEmbedding) {
    const QueryCommand* command = parseCommand("LOAD EMBEDDING FROM 'embeddings.csv' AS embedding", QueryCommand::Kind::LOAD_EMBEDDING_QUERY);
    EXPECT_EQ(static_cast<const LoadEmbeddingQuery*>(command)->getPropertyName(), "embedding");
}

TEST_F(KeywordObjectNameTest, searchesAVectorIndexS3) {
    const QueryCommand* command = parseCommand("VECTOR SEARCH IN s3 FOR 1 (0.1, 0.2) YIELD ids RETURN ids", QueryCommand::Kind::SINGLE_PART_QUERY);
    const Stmt* search = static_cast<const SinglePartQuery*>(command)->getStmts()->stmts().front();

    EXPECT_EQ(static_cast<const VectorSearchStmt*>(search)->getIndexName(), "s3");
}

TEST_F(KeywordObjectNameTest, installsAnExtensionGraph) {
    const QueryCommand* command = parseCommand("INSTALL graph", QueryCommand::Kind::INSTALL_EXTENSION_QUERY);
    EXPECT_EQ(static_cast<const InstallExtensionQuery*>(command)->getExtensionName(), "graph");
}

TEST_F(KeywordObjectNameTest, importsJsonlAsAGraphGraph) {
    const QueryCommand* command = parseCommand("LOAD JSONL 'f.jsonl' AS graph", QueryCommand::Kind::LOAD_JSONL_QUERY);
    EXPECT_EQ(static_cast<const LoadJsonlQuery*>(command)->getGraphName(), "graph");
}

TEST_F(KeywordObjectNameTest, importsGmlAsAGraphReturn) {
    const QueryCommand* command = parseCommand("LOAD GML 'f.gml' AS return", QueryCommand::Kind::LOAD_GML_QUERY);
    EXPECT_EQ(static_cast<const LoadGMLQuery*>(command)->getGraphName(), "return");
}

TEST_F(KeywordObjectNameTest, importsParquetAsAGraphMatch) {
    const QueryCommand* command = parseCommand("LOAD PARQUET 'f.parquet' AS match WITH DURATIONS [commit]", QueryCommand::Kind::LOAD_PARQUET_QUERY);
    const LoadParquetQuery* load = static_cast<const LoadParquetQuery*>(command);

    EXPECT_EQ(load->getGraphName(), "match");
    EXPECT_TRUE(load->getDurationSpecs().contains("commit"));
}

TEST_F(KeywordObjectNameTest, readsDatetimesCommit) {
    const QueryCommand* command = parseCommand("LOAD JSONL 'f.jsonl' WITH DATETIMES [commit, `created at`]", QueryCommand::Kind::LOAD_JSONL_QUERY);
    const LoadJsonlQuery* load = static_cast<const LoadJsonlQuery*>(command);

    EXPECT_TRUE(load->getDateTimeSpecs().contains("commit"));
    EXPECT_TRUE(load->getDateTimeSpecs().contains("created at"));
}

TEST_F(KeywordObjectNameTest, readsDurationsMatch) {
    const QueryCommand* command = parseCommand("LOAD JSONL 'f.jsonl' WITH DURATIONS [match, `time spent`]", QueryCommand::Kind::LOAD_JSONL_QUERY);
    const LoadJsonlQuery* load = static_cast<const LoadJsonlQuery*>(command);

    EXPECT_TRUE(load->getDurationSpecs().contains("match"));
    EXPECT_TRUE(load->getDurationSpecs().contains("time spent"));
}

TEST_F(KeywordObjectNameTest, readsEmbeddingsEmbedding) {
    const QueryCommand* command = parseCommand("LOAD JSONL 'f.jsonl' WITH EMBEDDINGS [{embedding, 4}, {`face vector`, 8}]", QueryCommand::Kind::LOAD_JSONL_QUERY);
    const EmbeddingsSpec& embeddings = static_cast<const LoadJsonlQuery*>(command)->getEmbeddingSpecs();

    const EmbeddingsSpec::ConstIterator embedding = embeddings.find("embedding");
    ASSERT_NE(embedding, embeddings.end());
    EXPECT_EQ(embedding->second, 4u);

    const EmbeddingsSpec::ConstIterator faceVector = embeddings.find("face vector");
    ASSERT_NE(faceVector, embeddings.end());
    EXPECT_EQ(faceVector->second, 8u);
}
