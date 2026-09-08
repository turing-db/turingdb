#include <gtest/gtest.h>

#include <math.h>
#include <stddef.h>

#include <array>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "ID.h"
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnOptVector.h"
#include "datapart/EdgeRecord.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Vector = std::array<float, 3>;

// The anchor points along the first axis, and each neighbour sits at a known angle from
// it, so the ranking a query answers is the order these vectors are written in.
constexpr Vector anchorVector {1.0f, 0.0f, 0.0f};
constexpr Vector nearVector {0.9f, 0.1f, 0.0f};
constexpr Vector diagonalVector {0.5f, 0.5f, 0.0f};
constexpr Vector orthogonalVector {0.0f, 1.0f, 0.0f};
constexpr Vector oppositeVector {-1.0f, 0.0f, 0.0f};
constexpr Vector nearTwoHopVector {0.8f, 0.2f, 0.1f};
constexpr Vector farTwoHopVector {0.1f, 0.1f, 0.9f};

// The engine scores in float and widens the result, so these double-precision references
// hold to a tolerance rather than exactly.
constexpr double scoreTolerance = 1e-6;

double cosineSimilarity(const Vector& a, const Vector& b) {
    double dot = 0.0;
    double normA = 0.0;
    double normB = 0.0;

    for (size_t element = 0; element < a.size(); element++) {
        const double left = static_cast<double>(a[element]);
        const double right = static_cast<double>(b[element]);

        dot += left * right;
        normA += left * left;
        normB += right * right;
    }

    return dot / (sqrt(normA) * sqrt(normB));
}

double euclideanDistance(const Vector& a, const Vector& b) {
    double sum = 0.0;

    for (size_t element = 0; element < a.size(); element++) {
        const double difference = static_cast<double>(a[element]) - static_cast<double>(b[element]);
        sum += difference * difference;
    }

    return sqrt(sum);
}

// Reads a ranking the way it was emitted - the neighbour's name beside the score it was
// ordered on, in row order - so a test asserts the ranking and not merely the set of rows
// it drew from.
class RankingSink : public NLOutputSink {
public:
    using Row = std::pair<std::string, std::optional<double>>;
    using Rows = std::vector<Row>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        using NameColumn = ColumnOptVector<types::String::Primitive>;
        using ScoreColumn = ColumnOptVector<double>;

        const NameColumn* const names = static_cast<const NameColumn*>(chunks[0]);
        const ScoreColumn* const scores = static_cast<const ScoreColumn*>(chunks[1]);

        const std::vector<std::optional<types::String::Primitive>>& rawNames = names->getRaw();
        const std::vector<std::optional<double>>& rawScores = scores->getRaw();

        for (size_t row = offset; row < offset + rowCount; row++) {
            ASSERT_TRUE(rawNames[row]);
            _rows.emplace_back(std::string {*rawNames[row]}, rawScores[row]);
        }
    }

    const Rows& getRows() const { return _rows; }

private:
    Rows _rows;
};

// The single value column an aggregate over a similarity ends on
class ScoreSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        using ScoreColumn = ColumnOptVector<double>;

        const ScoreColumn* const scores = static_cast<const ScoreColumn*>(chunks[0]);
        const std::vector<std::optional<double>>& rawScores = scores->getRaw();

        for (size_t row = offset; row < offset + rowCount; row++) {
            _scores.push_back(rawScores[row]);
        }
    }

    const std::vector<std::optional<double>>& getScores() const { return _scores; }

private:
    std::vector<std::optional<double>> _scores;
};

}

// Ranking the graph neighbourhood of a node by embedding similarity on the v3 engine: a
// traversal reaches the neighbours, cosine_similarity scores each against the anchor's own
// vector, and ORDER BY / LIMIT rank and cut the result.
//
// The anchor's vector rides the traversal's carry set, so a.vec and m.vec are read
// row-aligned and the score needs no cross product. The legacy planner cannot answer the
// WITH-threshold shape at all ("WITH not yet supported").
//
// Embeddings are the point here, and simpledb carries none, so the fixture is its own
// graph - 3-dimensional vectors on every node but Blank:
//
//   Anchor(:Anchor) -KNOWS-> Near, Diagonal, Orthogonal, Opposite   (:Person)
//   Anchor          -FOLLOWS-> Blank(:Blank, no vec)
//   Near -KNOWS-> NearTwoHop        Orthogonal -KNOWS-> FarTwoHop   (:Person)
//   Near -LIKES-> Anchor
class EmbeddingNeighbourRankingTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildEmbeddingGraph(system.createGraph(_graphName));
    }

    void runQuery(std::string_view query, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectRanking(std::string_view query, const RankingSink::Rows& expected) {
        RankingSink sink;
        runQuery(query, sink);

        const RankingSink::Rows& rows = sink.getRows();
        ASSERT_EQ(rows.size(), expected.size()) << "query: " << query;

        for (size_t row = 0; row < expected.size(); row++) {
            EXPECT_EQ(rows[row].first, expected[row].first) << "query: " << query << ", row " << row;

            ASSERT_EQ(rows[row].second.has_value(), expected[row].second.has_value())
                << "query: " << query << ", row " << row;

            if (expected[row].second) {
                EXPECT_NEAR(*rows[row].second, *expected[row].second, scoreTolerance)
                    << "query: " << query << ", row " << row;
            }
        }
    }

    void expectNames(std::string_view query, const std::vector<std::string>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        const std::vector<StringRowSink::Row>& rows = sink.getRows();
        ASSERT_EQ(rows.size(), expected.size()) << "query: " << query;

        for (size_t row = 0; row < expected.size(); row++) {
            const StringRowSink::Row expectedRow {expected[row]};
            EXPECT_EQ(rows[row], expectedRow) << "query: " << query << ", row " << row;
        }
    }

    void expectScore(std::string_view query, double expected) {
        ScoreSink sink;
        runQuery(query, sink);

        const std::vector<std::optional<double>>& scores = sink.getScores();
        ASSERT_EQ(scores.size(), 1u) << "query: " << query;
        ASSERT_TRUE(scores.front()) << "query: " << query;

        EXPECT_NEAR(*scores.front(), expected, scoreTolerance) << "query: " << query;
    }

    const std::string _graphName = "embeddings";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;

private:
    void buildEmbeddingGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const auto vectorNode = [&](std::string_view label, std::string_view name, const Vector& vector) -> NodeID {
            const NodeID node = writer.addNode({label});
            writer.addNodeProperty<types::String>(node, "name", types::String::Primitive {name});
            writer.addNodeProperty<types::Embedding>(node, "vec", types::Embedding::Primitive {vector});
            return node;
        };

        const NodeID anchor = vectorNode("Anchor", "Anchor", anchorVector);
        const NodeID near = vectorNode("Person", "Near", nearVector);
        const NodeID diagonal = vectorNode("Person", "Diagonal", diagonalVector);
        const NodeID orthogonal = vectorNode("Person", "Orthogonal", orthogonalVector);
        const NodeID opposite = vectorNode("Person", "Opposite", oppositeVector);
        const NodeID nearTwoHop = vectorNode("Person", "NearTwoHop", nearTwoHopVector);
        const NodeID farTwoHop = vectorNode("Person", "FarTwoHop", farTwoHopVector);

        const NodeID blank = writer.addNode({"Blank"});
        writer.addNodeProperty<types::String>(blank, "name", types::String::Primitive {"Blank"});

        writer.addEdge("KNOWS", anchor, near);
        writer.addEdge("KNOWS", anchor, diagonal);
        writer.addEdge("KNOWS", anchor, orthogonal);
        writer.addEdge("KNOWS", anchor, opposite);
        writer.addEdge("KNOWS", near, nearTwoHop);
        writer.addEdge("KNOWS", orthogonal, farTwoHop);
        writer.addEdge("FOLLOWS", anchor, blank);
        writer.addEdge("LIKES", near, anchor);

        writer.submit();
        jobSystem.terminate();
    }
};

TEST_F(EmbeddingNeighbourRankingTest, ranksTheNeighboursByCosineSimilarity) {
    const RankingSink::Rows expected {
        {"Near", cosineSimilarity(anchorVector, nearVector)},
        {"Diagonal", cosineSimilarity(anchorVector, diagonalVector)},
        {"Orthogonal", cosineSimilarity(anchorVector, orthogonalVector)},
        {"Opposite", cosineSimilarity(anchorVector, oppositeVector)},
    };

    expectRanking("MATCH (a:Anchor)-[:KNOWS]->(m) "
                  "RETURN m.name, cosine_similarity(a.vec, m.vec) AS sim "
                  "ORDER BY sim DESC",
                  expected);
}

TEST_F(EmbeddingNeighbourRankingTest, ranksTheNeighboursByEuclideanDistance) {
    const RankingSink::Rows expected {
        {"Near", euclideanDistance(anchorVector, nearVector)},
        {"Diagonal", euclideanDistance(anchorVector, diagonalVector)},
        {"Orthogonal", euclideanDistance(anchorVector, orthogonalVector)},
        {"Opposite", euclideanDistance(anchorVector, oppositeVector)},
    };

    expectRanking("MATCH (a:Anchor)-[:KNOWS]->(m) "
                  "RETURN m.name, euclidean_distance(a.vec, m.vec) AS distance "
                  "ORDER BY distance ASC",
                  expected);
}

TEST_F(EmbeddingNeighbourRankingTest, takesTheTopKOfTheRanking) {
    const RankingSink::Rows expected {
        {"Near", cosineSimilarity(anchorVector, nearVector)},
        {"Diagonal", cosineSimilarity(anchorVector, diagonalVector)},
    };

    expectRanking("MATCH (a:Anchor)-[:KNOWS]->(m) "
                  "RETURN m.name, cosine_similarity(a.vec, m.vec) AS sim "
                  "ORDER BY sim DESC LIMIT 2",
                  expected);
}

TEST_F(EmbeddingNeighbourRankingTest, windowsTheRankingWithSkipAndLimit) {
    const RankingSink::Rows expected {
        {"Diagonal", cosineSimilarity(anchorVector, diagonalVector)},
        {"Orthogonal", cosineSimilarity(anchorVector, orthogonalVector)},
    };

    expectRanking("MATCH (a:Anchor)-[:KNOWS]->(m) "
                  "RETURN m.name, cosine_similarity(a.vec, m.vec) AS sim "
                  "ORDER BY sim DESC SKIP 1 LIMIT 2",
                  expected);
}

// A top-K ranking keeps only its best rows rather than sorting the whole neighbourhood:
// the LIMIT lands on the accumulator the sort collects into, not on a pass after it.
TEST_F(EmbeddingNeighbourRankingTest, foldsTheTopKIntoTheSortBuffer) {
    StringRowSink sink;
    runQuery("EXPLAIN (nl) MATCH (a:Anchor)-[:KNOWS]->(m) "
             "RETURN m.name, cosine_similarity(a.vec, m.vec) AS sim "
             "ORDER BY sim DESC LIMIT 2",
             sink);

    const std::vector<StringRowSink::Row>& rows = sink.getRows();
    ASSERT_EQ(rows.size(), 1u);
    ASSERT_EQ(rows.front().size(), 2u);

    const std::string& dump = rows.front()[1];
    EXPECT_NE(dump.find("nl.sort_buffer keys [1] ascending [false] limit 2"), std::string::npos) << dump;
}

TEST_F(EmbeddingNeighbourRankingTest, ranksOnAScoreTheProjectionDoesNotReturn) {
    const std::vector<std::string> expected {"Near", "Diagonal", "Orthogonal", "Opposite"};

    expectNames("MATCH (a:Anchor)-[:KNOWS]->(m) "
                "RETURN m.name "
                "ORDER BY cosine_similarity(a.vec, m.vec) DESC",
                expected);
}

TEST_F(EmbeddingNeighbourRankingTest, ranksTheSecondHopNeighbourhood) {
    const RankingSink::Rows expected {
        {"NearTwoHop", cosineSimilarity(anchorVector, nearTwoHopVector)},
        {"FarTwoHop", cosineSimilarity(anchorVector, farTwoHopVector)},
    };

    expectRanking("MATCH (a:Anchor)-[:KNOWS]->()-[:KNOWS]->(m) "
                  "RETURN m.name, cosine_similarity(a.vec, m.vec) AS sim "
                  "ORDER BY sim DESC",
                  expected);
}

TEST_F(EmbeddingNeighbourRankingTest, filtersTheRankingByASimilarityThreshold) {
    const RankingSink::Rows expected {
        {"Near", cosineSimilarity(anchorVector, nearVector)},
        {"Diagonal", cosineSimilarity(anchorVector, diagonalVector)},
    };

    expectRanking("MATCH (a:Anchor)-[:KNOWS]->(m) "
                  "WITH m, cosine_similarity(a.vec, m.vec) AS sim "
                  "WHERE sim > 0.5 "
                  "RETURN m.name, sim "
                  "ORDER BY sim DESC",
                  expected);
}

TEST_F(EmbeddingNeighbourRankingTest, averagesTheSimilarityOfTheNeighbourhood) {
    const double total = cosineSimilarity(anchorVector, nearVector)
        + cosineSimilarity(anchorVector, diagonalVector)
        + cosineSimilarity(anchorVector, orthogonalVector)
        + cosineSimilarity(anchorVector, oppositeVector);

    expectScore("MATCH (a:Anchor)-[:KNOWS]->(m) "
                "RETURN avg(cosine_similarity(a.vec, m.vec)) AS meanSimilarity",
                total / 4.0);
}

// The same scoring with a query vector spelled out instead of read from the anchor, over
// every node rather than a neighbourhood.
TEST_F(EmbeddingNeighbourRankingTest, ranksEveryNodeAgainstALiteralQueryVector) {
    const RankingSink::Rows expected {
        {"Near", cosineSimilarity(anchorVector, nearVector)},
        {"NearTwoHop", cosineSimilarity(anchorVector, nearTwoHopVector)},
        {"Diagonal", cosineSimilarity(anchorVector, diagonalVector)},
    };

    expectRanking("MATCH (n:Person) "
                  "RETURN n.name, cosine_similarity(n.vec, (1.0, 0.0, 0.0)) AS sim "
                  "ORDER BY sim DESC LIMIT 3",
                  expected);
}

// A neighbour carrying no vector scores null, and null orders above every value, so a
// descending ranking leads with it - the same place Cypher puts it.
TEST_F(EmbeddingNeighbourRankingTest, leadsTheDescendingRankingWithTheUnscoredNeighbour) {
    const RankingSink::Rows expected {
        {"Blank", std::nullopt},
        {"Near", cosineSimilarity(anchorVector, nearVector)},
        {"Diagonal", cosineSimilarity(anchorVector, diagonalVector)},
        {"Orthogonal", cosineSimilarity(anchorVector, orthogonalVector)},
        {"Opposite", cosineSimilarity(anchorVector, oppositeVector)},
    };

    expectRanking("MATCH (a:Anchor)-[e]->(m) "
                  "RETURN m.name, cosine_similarity(a.vec, m.vec) AS sim "
                  "ORDER BY sim DESC",
                  expected);
}

TEST_F(EmbeddingNeighbourRankingTest, dropsTheUnscoredNeighbourByANullTestOnTheScore) {
    const RankingSink::Rows expected {
        {"Near", cosineSimilarity(anchorVector, nearVector)},
        {"Diagonal", cosineSimilarity(anchorVector, diagonalVector)},
        {"Orthogonal", cosineSimilarity(anchorVector, orthogonalVector)},
        {"Opposite", cosineSimilarity(anchorVector, oppositeVector)},
    };

    expectRanking("MATCH (a:Anchor)-[e]->(m) "
                  "WITH m, cosine_similarity(a.vec, m.vec) AS sim "
                  "WHERE sim IS NOT NULL "
                  "RETURN m.name, sim "
                  "ORDER BY sim DESC",
                  expected);
}

// Near is reached twice over an undirected match - out over KNOWS and back over LIKES - and
// ranks once.
TEST_F(EmbeddingNeighbourRankingTest, ranksEachNeighbourOnceOverEitherDirection) {
    const RankingSink::Rows expected {
        {"Blank", std::nullopt},
        {"Near", cosineSimilarity(anchorVector, nearVector)},
        {"Diagonal", cosineSimilarity(anchorVector, diagonalVector)},
        {"Orthogonal", cosineSimilarity(anchorVector, orthogonalVector)},
        {"Opposite", cosineSimilarity(anchorVector, oppositeVector)},
    };

    expectRanking("MATCH (a:Anchor)-[e]-(m) "
                  "RETURN DISTINCT m.name, cosine_similarity(a.vec, m.vec) AS sim "
                  "ORDER BY sim DESC",
                  expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
