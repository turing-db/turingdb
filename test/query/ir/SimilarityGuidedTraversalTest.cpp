#include <gtest/gtest.h>

#include <math.h>
#include <stddef.h>

#include <algorithm>
#include <array>
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

#include "BioAssert.h"
#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

struct Document {
    std::string_view _name;
    std::array<float, 4> _vector;
};

struct Link {
    std::string_view _source;
    std::string_view _target;
};

const std::array<Document, 12> documents {{
    {"d0",  {1.0f,  0.0f,  0.0f, 0.0f}},
    {"d1",  {0.9f,  0.3f,  0.0f, 0.0f}},
    {"d2",  {0.2f,  0.9f,  0.1f, 0.0f}},
    {"d3",  {0.0f,  0.1f,  0.9f, 0.2f}},
    {"d4",  {0.8f,  0.5f,  0.1f, 0.0f}},
    {"d5",  {0.1f,  0.0f,  0.2f, 0.9f}},
    {"d6",  {0.95f, 0.1f,  0.1f, 0.0f}},
    {"d7",  {0.3f,  0.8f,  0.3f, 0.1f}},
    {"d8",  {0.99f, 0.05f, 0.0f, 0.0f}},
    {"d9",  {0.4f,  0.4f,  0.7f, 0.1f}},
    {"d10", {0.7f,  0.6f,  0.2f, 0.0f}},
    {"d11", {0.0f,  0.0f,  0.1f, 1.0f}},
}};

const std::array<Link, 21> links {{
    {"d0", "d1"},  {"d0", "d2"},  {"d0", "d3"},  {"d0", "d5"},
    {"d1", "d4"},  {"d1", "d6"},  {"d1", "d7"},
    {"d2", "d7"},  {"d2", "d9"},
    {"d3", "d9"},  {"d3", "d11"},
    {"d4", "d8"},  {"d4", "d10"},
    {"d5", "d11"},
    {"d6", "d8"},  {"d6", "d10"},
    {"d7", "d9"},  {"d7", "d2"},
    {"d9", "d10"}, {"d10", "d8"}, {"d11", "d5"},
}};

// Two scores closer than this cannot be told apart from the engine's float arithmetic, so a
// cut between them would rank on nothing. Every ranking the tests compare stays clear of it.
constexpr double scoreSeparation = 1e-6;

enum class Reference {
    Origin,
    PreviousNode,
};

struct Hop {
    size_t _limit {0};
    bool _distinct {false};
};

struct Step {
    size_t _node {0};
    size_t _previous {0};
    double _score {0.0};
};

double cosineSimilarity(std::span<const float> left, std::span<const float> right) {
    double dot = 0.0;
    double leftNorm = 0.0;
    double rightNorm = 0.0;

    for (size_t element = 0; element < left.size(); element++) {
        dot += static_cast<double>(left[element]) * right[element];
        leftNorm += static_cast<double>(left[element]) * left[element];
        rightNorm += static_cast<double>(right[element]) * right[element];
    }

    return dot / (sqrt(leftNorm) * sqrt(rightNorm));
}

size_t findDocument(std::string_view name) {
    const auto document = std::ranges::find(documents, name, &Document::_name);
    bioassert(document != documents.end(), "No document named '{}'", name);

    return static_cast<size_t>(std::distance(documents.begin(), document));
}

std::span<const float> documentVector(std::string_view name) {
    return documents[findDocument(name)]._vector;
}

void buildAdjacency(std::vector<std::vector<size_t>>& adjacency) {
    adjacency.assign(documents.size(), {});

    for (const Link& link : links) {
        adjacency[findDocument(link._source)].push_back(findDocument(link._target));
    }
}

void toRows(std::span<const size_t> nodes, Rows& rows) {
    rows.clear();

    for (const size_t node : nodes) {
        rows.push_back({std::string {documents[node]._name}});
    }
}

// Replays a similarity-guided walk over the fixture data above, one hop at a time, in the
// order the query applies them: expand the frontier, deduplicate it if the WITH says
// DISTINCT, score every candidate, then rank and cut.
class BruteForceWalk {
public:
    BruteForceWalk(std::string_view seedName, Reference reference, std::span<const float> origin)
        : _origin(origin.begin(), origin.end())
        , _seed(findDocument(seedName))
        , _reference(reference)
    {
        buildAdjacency(_adjacency);
    }

    void run(std::span<const Hop> hops, Rows& rows) const {
        std::vector<Step> frontier {Step {_seed, _seed, 0.0}};

        for (const Hop& hop : hops) {
            std::vector<Step> candidates;
            expand(frontier, candidates);

            if (hop._distinct) {
                deduplicate(candidates);
            }

            score(candidates);
            rankAndCut(candidates, hop);

            frontier.swap(candidates);
        }

        std::vector<size_t> nodes;
        for (const Step& step : frontier) {
            nodes.push_back(step._node);
        }

        toRows(nodes, rows);
    }

    void collectEndpoints(size_t hopCount, Rows& rows) const {
        std::vector<size_t> frontier {_seed};

        for (size_t hop = 0; hop < hopCount; hop++) {
            std::vector<size_t> reached;
            for (const size_t node : frontier) {
                const std::vector<size_t>& targets = _adjacency[node];
                reached.insert(reached.end(), targets.begin(), targets.end());
            }

            frontier.swap(reached);
        }

        std::ranges::sort(frontier);
        const auto duplicates = std::ranges::unique(frontier);
        frontier.erase(duplicates.begin(), duplicates.end());

        toRows(frontier, rows);
        std::ranges::sort(rows);
    }

private:
    std::vector<std::vector<size_t>> _adjacency;
    std::vector<float> _origin;
    size_t _seed {0};
    Reference _reference {Reference::Origin};

    void expand(std::span<const Step> frontier, std::vector<Step>& candidates) const {
        candidates.clear();

        for (const Step& step : frontier) {
            for (const size_t target : _adjacency[step._node]) {
                candidates.push_back(Step {target, step._node, 0.0});
            }
        }
    }

    void deduplicate(std::vector<Step>& candidates) const {
        std::vector<Step> kept;

        for (const Step& candidate : candidates) {
            const bool alreadyKept = std::ranges::any_of(kept, [&candidate](const Step& step) {
                return step._node == candidate._node;
            });

            if (!alreadyKept) {
                kept.push_back(candidate);
            }
        }

        candidates.swap(kept);
    }

    void score(std::vector<Step>& candidates) const {
        for (Step& candidate : candidates) {
            std::span<const float> reference {_origin};
            if (_reference == Reference::PreviousNode) {
                reference = documents[candidate._previous]._vector;
            }

            candidate._score = cosineSimilarity(documents[candidate._node]._vector, reference);
        }
    }

    void rankAndCut(std::vector<Step>& candidates, const Hop& hop) const {
        std::ranges::sort(candidates, std::ranges::greater {}, &Step::_score);

        size_t kept = candidates.size();
        if (hop._limit != 0) {
            kept = std::min(hop._limit, candidates.size());
        }

        for (size_t index = 0; index < kept && index + 1 < candidates.size(); index++) {
            EXPECT_GT(candidates[index]._score - candidates[index + 1]._score, scoreSeparation)
                << "the walk ranks " << documents[candidates[index]._node]._name << " and "
                << documents[candidates[index + 1]._node]._name << " on nothing";
        }

        candidates.resize(kept);
    }
};

}

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
    void buildDocumentGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        std::vector<NodeID> nodes;
        for (const Document& document : documents) {
            const NodeID node = writer.addNode({"Doc"});
            writer.addNodeProperty<types::String>(node, "name", types::String::Primitive {document._name});
            writer.addNodeProperty<types::Embedding>(node, "vec", std::span<const float> {document._vector});
            nodes.push_back(node);
        }

        for (const Link& link : links) {
            writer.addEdge("LINKS_TO", nodes[findDocument(link._source)], nodes[findDocument(link._target)]);
        }

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
    const std::array<float, 4> origin {1.0f, 0.0f, 0.0f, 0.0f};
    const std::array<Hop, 1> hops {{{0, false}}};

    Rows expected;
    BruteForceWalk {"d0", Reference::Origin, origin}.run(hops, expected);

    expectRowsInOrder("MATCH (seed:Doc {name: 'd0'})-[:LINKS_TO]->(hop:Doc) "
                      "RETURN hop.name "
                      "ORDER BY cosine_similarity(hop.vec, (1.0, 0.0, 0.0, 0.0)) DESC",
                      expected);
}

TEST_F(SimilarityGuidedTraversalTest, keepsTheMostSimilarNodesAtEveryHop) {
    const std::array<float, 4> origin {1.0f, 0.0f, 0.0f, 0.0f};
    const std::array<Hop, 3> hops {{{2, false}, {2, true}, {3, true}}};

    Rows expected;
    BruteForceWalk {"d0", Reference::Origin, origin}.run(hops, expected);

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

TEST_F(SimilarityGuidedTraversalTest, theUnprunedWalkReachesMoreThanTheBeam) {
    const std::array<float, 4> origin {1.0f, 0.0f, 0.0f, 0.0f};

    Rows expected;
    BruteForceWalk {"d0", Reference::Origin, origin}.collectEndpoints(3, expected);

    expectRows("MATCH (:Doc {name: 'd0'})-[:LINKS_TO]->(:Doc)-[:LINKS_TO]->(:Doc)-[:LINKS_TO]->(h3:Doc) "
               "RETURN DISTINCT h3.name",
               expected);
}

TEST_F(SimilarityGuidedTraversalTest, steersTheWalkWithTheQueryVector) {
    const std::array<float, 4> origin {0.0f, 0.0f, 1.0f, 0.0f};
    const std::array<Hop, 3> hops {{{2, false}, {2, true}, {0, true}}};

    Rows expected;
    BruteForceWalk {"d0", Reference::Origin, origin}.run(hops, expected);
    std::ranges::sort(expected);

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
    const std::array<Hop, 3> hops {{{2, false}, {2, true}, {3, true}}};

    Rows expected;
    BruteForceWalk {"d0", Reference::Origin, documentVector("d0")}.run(hops, expected);

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

TEST_F(SimilarityGuidedTraversalTest, ranksEachHopAgainstThePreviousNode) {
    const std::array<Hop, 3> hops {{{2, false}, {2, false}, {3, false}}};

    Rows expected;
    BruteForceWalk {"d2", Reference::PreviousNode, {}}.run(hops, expected);

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

TEST_F(SimilarityGuidedTraversalTest, ranksEveryHopAgainstTheSeedInstead) {
    const std::array<Hop, 3> hops {{{2, false}, {2, false}, {3, false}}};

    Rows expected;
    BruteForceWalk {"d2", Reference::Origin, documentVector("d2")}.run(hops, expected);

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
