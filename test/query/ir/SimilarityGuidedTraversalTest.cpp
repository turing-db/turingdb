#include <gtest/gtest.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "ID.h"
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
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

using Rows = std::vector<StringRowSink::Row>;

}

// Similarities of the twelve documents to (1, 0, 0, 0), which is d0's own vector:
// d8 .999, d6 .989, d1 .949, d4 .843, d10 .742, d9 .442, d7 .329, d2 .216, d5 .108,
// d3 and d11 zero.
class SimilarityGuidedTraversalTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildDocumentGraph(system.createGraph(_graphName));
    }

    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    const std::string _graphName = "documents";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;

private:
    //   d0 -> d1 d2 d3 d5     d4 -> d8 d10     d9  -> d10
    //   d1 -> d4 d6 d7        d5 -> d11        d10 -> d8
    //   d2 -> d7 d9           d6 -> d8 d10     d11 -> d5
    //   d3 -> d9 d11          d7 -> d9 d2
    void buildDocumentGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const auto document = [&](std::string_view name, const std::vector<float>& vector) -> NodeID {
            const NodeID node = writer.addNode({"Doc"});
            writer.addNodeProperty<types::String>(node, "name", types::String::Primitive {name});
            writer.addNodeProperty<types::Embedding>(node, "vec", std::span<const float> {vector});
            return node;
        };

        const NodeID doc0 = document("d0", {1.0f, 0.0f, 0.0f, 0.0f});
        const NodeID doc1 = document("d1", {0.9f, 0.3f, 0.0f, 0.0f});
        const NodeID doc2 = document("d2", {0.2f, 0.9f, 0.1f, 0.0f});
        const NodeID doc3 = document("d3", {0.0f, 0.1f, 0.9f, 0.2f});
        const NodeID doc4 = document("d4", {0.8f, 0.5f, 0.1f, 0.0f});
        const NodeID doc5 = document("d5", {0.1f, 0.0f, 0.2f, 0.9f});
        const NodeID doc6 = document("d6", {0.95f, 0.1f, 0.1f, 0.0f});
        const NodeID doc7 = document("d7", {0.3f, 0.8f, 0.3f, 0.1f});
        const NodeID doc8 = document("d8", {0.99f, 0.05f, 0.0f, 0.0f});
        const NodeID doc9 = document("d9", {0.4f, 0.4f, 0.7f, 0.1f});
        const NodeID doc10 = document("d10", {0.7f, 0.6f, 0.2f, 0.0f});
        const NodeID doc11 = document("d11", {0.0f, 0.0f, 0.1f, 1.0f});

        const auto link = [&](NodeID source, NodeID target) {
            writer.addEdge("LINKS_TO", source, target);
        };

        link(doc0, doc1);
        link(doc0, doc2);
        link(doc0, doc3);
        link(doc0, doc5);

        link(doc1, doc4);
        link(doc1, doc6);
        link(doc1, doc7);

        link(doc2, doc7);
        link(doc2, doc9);

        link(doc3, doc9);
        link(doc3, doc11);

        link(doc4, doc8);
        link(doc4, doc10);

        link(doc5, doc11);

        link(doc6, doc8);
        link(doc6, doc10);

        link(doc7, doc9);
        link(doc7, doc2);

        link(doc9, doc10);
        link(doc10, doc8);
        link(doc11, doc5);

        writer.submit();
        jobSystem.terminate();
    }

    void runQuery(std::string_view query, StringRowSink& sink) {
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
};

TEST_F(SimilarityGuidedTraversalTest, ranksTheFirstHopFrontierBySimilarity) {
    const Rows expected {{"d1"}, {"d2"}, {"d5"}, {"d3"}};

    expectRowsInOrder("MATCH (seed:Doc {name: 'd0'})-[:LINKS_TO]->(hop:Doc) "
                      "RETURN hop.name "
                      "ORDER BY cosine_similarity(hop.vec, (1.0, 0.0, 0.0, 0.0)) DESC",
                      expected);
}

// d1 and d2 survive the first hop, d6 and d4 the second, so only their targets are reached.
TEST_F(SimilarityGuidedTraversalTest, keepsTheMostSimilarNodesAtEveryHop) {
    const Rows expected {{"d8"}, {"d10"}};

    expectRowsInOrder("WITH (1.0, 0.0, 0.0, 0.0) AS origin "
                      "MATCH (seed:Doc {name: 'd0'})-[:LINKS_TO]->(h1:Doc) "
                      "WITH origin, h1, cosine_similarity(h1.vec, origin) AS sim1 "
                      "ORDER BY sim1 DESC "
                      "LIMIT 2 "
                      "MATCH (h1)-[:LINKS_TO]->(h2:Doc) "
                      "WITH DISTINCT origin, h2, cosine_similarity(h2.vec, origin) AS sim2 "
                      "ORDER BY sim2 DESC "
                      "LIMIT 2 "
                      "MATCH (h2)-[:LINKS_TO]->(h3:Doc) "
                      "WITH DISTINCT origin, h3, cosine_similarity(h3.vec, origin) AS sim3 "
                      "ORDER BY sim3 DESC "
                      "LIMIT 3 "
                      "RETURN h3.name",
                      expected);
}

// The same three hops with nothing pruned reach five documents; the pruned walk above
// visits the two of them the query vector ranks highest.
TEST_F(SimilarityGuidedTraversalTest, theUnprunedWalkReachesMoreThanTheBeam) {
    const Rows expected {{"d10"}, {"d2"}, {"d5"}, {"d8"}, {"d9"}};

    expectRows("MATCH (:Doc {name: 'd0'})-[:LINKS_TO]->(:Doc)-[:LINKS_TO]->(:Doc)-[:LINKS_TO]->(h3:Doc) "
               "RETURN DISTINCT h3.name",
               expected);
}

// (0, 0, 1, 0) sends the same walk down d3 and d5 instead of d1 and d2.
TEST_F(SimilarityGuidedTraversalTest, steersTheWalkWithTheQueryVector) {
    const Rows expected {{"d10"}, {"d5"}};

    expectRows("WITH (0.0, 0.0, 1.0, 0.0) AS origin "
               "MATCH (seed:Doc {name: 'd0'})-[:LINKS_TO]->(h1:Doc) "
               "WITH origin, h1, cosine_similarity(h1.vec, origin) AS sim1 "
               "ORDER BY sim1 DESC "
               "LIMIT 2 "
               "MATCH (h1)-[:LINKS_TO]->(h2:Doc) "
               "WITH DISTINCT origin, h2, cosine_similarity(h2.vec, origin) AS sim2 "
               "ORDER BY sim2 DESC "
               "LIMIT 2 "
               "MATCH (h2)-[:LINKS_TO]->(h3:Doc) "
               "RETURN DISTINCT h3.name",
               expected);
}

TEST_F(SimilarityGuidedTraversalTest, scoresTheFrontierAgainstTheAnchorsEmbedding) {
    const Rows expected {{"d8"}, {"d10"}};

    expectRowsInOrder("MATCH (seed:Doc {name: 'd0'})-[:LINKS_TO]->(h1:Doc) "
                      "WITH seed, h1, cosine_similarity(h1.vec, seed.vec) AS sim1 "
                      "ORDER BY sim1 DESC "
                      "LIMIT 2 "
                      "MATCH (h1)-[:LINKS_TO]->(h2:Doc) "
                      "WITH DISTINCT seed, h2, cosine_similarity(h2.vec, seed.vec) AS sim2 "
                      "ORDER BY sim2 DESC "
                      "LIMIT 2 "
                      "MATCH (h2)-[:LINKS_TO]->(h3:Doc) "
                      "WITH DISTINCT h3, cosine_similarity(h3.vec, seed.vec) AS sim3 "
                      "ORDER BY sim3 DESC "
                      "LIMIT 3 "
                      "RETURN h3.name",
                      expected);
}

// The reference vector moves with the frontier: every hop scores its neighbours against
// the node it was reached from, not against the seed.
TEST_F(SimilarityGuidedTraversalTest, ranksEachHopAgainstThePreviousNode) {
    const Rows expected {{"d7"}, {"d10"}, {"d9"}};

    expectRowsInOrder("MATCH (seed:Doc {name: 'd2'})-[:LINKS_TO]->(h1:Doc) "
                      "WITH h1, cosine_similarity(h1.vec, seed.vec) AS sim1 "
                      "ORDER BY sim1 DESC "
                      "LIMIT 2 "
                      "MATCH (h1)-[:LINKS_TO]->(h2:Doc) "
                      "WITH h1, h2, cosine_similarity(h2.vec, h1.vec) AS sim2 "
                      "ORDER BY sim2 DESC "
                      "LIMIT 2 "
                      "MATCH (h2)-[:LINKS_TO]->(h3:Doc) "
                      "WITH h2, h3, cosine_similarity(h3.vec, h2.vec) AS sim3 "
                      "ORDER BY sim3 DESC "
                      "LIMIT 3 "
                      "RETURN h3.name",
                      expected);
}

// The same walk with the reference pinned to the seed keeps d10 over d9 at the second hop,
// and ends somewhere else for it.
TEST_F(SimilarityGuidedTraversalTest, ranksEveryHopAgainstTheSeedInstead) {
    const Rows expected {{"d7"}, {"d9"}, {"d8"}};

    expectRowsInOrder("MATCH (seed:Doc {name: 'd2'})-[:LINKS_TO]->(h1:Doc) "
                      "WITH seed, h1, cosine_similarity(h1.vec, seed.vec) AS sim1 "
                      "ORDER BY sim1 DESC "
                      "LIMIT 2 "
                      "MATCH (h1)-[:LINKS_TO]->(h2:Doc) "
                      "WITH seed, h2, cosine_similarity(h2.vec, seed.vec) AS sim2 "
                      "ORDER BY sim2 DESC "
                      "LIMIT 2 "
                      "MATCH (h2)-[:LINKS_TO]->(h3:Doc) "
                      "WITH h3, cosine_similarity(h3.vec, seed.vec) AS sim3 "
                      "ORDER BY sim3 DESC "
                      "LIMIT 3 "
                      "RETURN h3.name",
                      expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
