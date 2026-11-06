#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "ID.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "JobSystem.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// DISTINCT, grouping and count(DISTINCT) key an embedding by its floats, wherever it
// sits: a column, a list element or a map value. Alpha and Beta carry one
// vector, Gamma the same floats with the sign of a zero flipped, Delta another vector,
// and Plain none.
class EmbeddingDistinctTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildEmbeddingGraph(system.createGraph(_graphName));
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "embeddings";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;

private:
    void buildEmbeddingGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const auto vectorNode = [&](std::string_view name, const std::vector<float>& vector) {
            const NodeID node = writer.addNode({"Doc"});
            writer.addNodeProperty<types::String>(node, "name", types::String::Primitive {name});
            writer.addNodeProperty<types::Embedding>(node, "vec", types::Embedding::Primitive {vector});
        };

        vectorNode("Alpha", {1.0f, 0.0f});
        vectorNode("Beta", {1.0f, 0.0f});
        vectorNode("Gamma", {1.0f, -0.0f});
        vectorNode("Delta", {2.0f, 5.0f});

        const NodeID plain = writer.addNode({"Doc"});
        writer.addNodeProperty<types::String>(plain, "name", types::String::Primitive {"Plain"});

        writer.submit();

        jobSystem.terminate();
    }
};

TEST_F(EmbeddingDistinctTest, comparesAnEmbeddingInsideAListOrAMap) {
    expectRows("MATCH (a:Doc {name: 'Alpha'}), (b:Doc) WHERE b.vec IS NOT NULL RETURN b.name, [a.vec] = [b.vec], {e: a.vec} = {e: b.vec}",
               {{"Alpha", "true", "true"},
                {"Beta", "true", "true"},
                {"Gamma", "true", "true"},
                {"Delta", "false", "false"}});
}

TEST_F(EmbeddingDistinctTest, removesDuplicateEmbeddings) {
    expectRows("MATCH (d:Doc) WITH DISTINCT d.vec AS v RETURN count(*)", {{"3"}});
}

TEST_F(EmbeddingDistinctTest, removesDuplicateEmbeddingsInsideAList) {
    expectRows("MATCH (d:Doc) UNWIND [[d.vec]] AS x WITH DISTINCT x RETURN count(*)", {{"3"}});
}

TEST_F(EmbeddingDistinctTest, removesDuplicateEmbeddingsInsideAMap) {
    expectRows("MATCH (d:Doc) UNWIND [{e: d.vec}] AS x WITH DISTINCT x RETURN count(*)", {{"3"}});
}

TEST_F(EmbeddingDistinctTest, countsDistinctEmbeddings) {
    expectRows("MATCH (d:Doc) RETURN count(DISTINCT d.vec)", {{"2"}});
}

TEST_F(EmbeddingDistinctTest, countsDistinctEmbeddingsPerGroup) {
    expectRows("MATCH (d:Doc) WITH d.vec IS NULL AS missing, count(DISTINCT d.vec) AS c RETURN missing, c",
               {{"false", "2"}, {"true", "0"}});
}

TEST_F(EmbeddingDistinctTest, groupsByAnEmbedding) {
    expectRows("MATCH (d:Doc) WITH d.vec AS v, count(*) AS c RETURN c", {{"3"}, {"1"}, {"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
