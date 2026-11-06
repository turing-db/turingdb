#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
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

// Alpha and Beta carry one vector, Gamma the same floats with the sign of a zero flipped,
// Delta another vector, and Plain none. The two Odd nodes hold NaNs of opposite signs.
class EmbeddingCollectTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        SystemManager& systemManager = _env->getSystemManager();
        _interpreter = std::make_unique<QueryInterpreterV3>(&systemManager, &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = systemManager.accessUnique();
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

        const auto vectorNode = [&](std::string_view label, std::string_view name, const std::vector<float>& vector) {
            const NodeID node = writer.addNode({label});
            writer.addNodeProperty<types::String>(node, "name", types::String::Primitive {name});
            writer.addNodeProperty<types::Embedding>(node, "vec", types::Embedding::Primitive {vector});
        };

        vectorNode("Doc", "Alpha", {1.0f, 0.0f});
        vectorNode("Doc", "Beta", {1.0f, 0.0f});
        vectorNode("Doc", "Gamma", {1.0f, -0.0f});
        vectorNode("Doc", "Delta", {2.0f, 5.0f});

        const float nan = std::numeric_limits<float>::quiet_NaN();
        vectorNode("Odd", "PositiveNaN", {nan, 1.0f});
        vectorNode("Odd", "NegativeNaN", {-nan, 1.0f});

        const NodeID plain = writer.addNode({"Doc"});
        writer.addNodeProperty<types::String>(plain, "name", types::String::Primitive {"Plain"});

        writer.submit();

        jobSystem.terminate();
    }
};

TEST_F(EmbeddingCollectTest, collectsEmbeddings) {
    expectRows("MATCH (d:Doc) WITH collect(d.vec) AS vs MATCH (a:Doc) WHERE a.vec IS NOT NULL RETURN a.name, size(vs), size([v IN vs WHERE v = a.vec])",
               {{"Alpha", "4", "3"},
                {"Beta", "4", "3"},
                {"Gamma", "4", "3"},
                {"Delta", "4", "1"}});
}

TEST_F(EmbeddingCollectTest, collectsDistinctEmbeddings) {
    expectRows("MATCH (d:Doc) WITH collect(DISTINCT d.vec) AS vs MATCH (a:Doc) WHERE a.vec IS NOT NULL RETURN a.name, size(vs), size([v IN vs WHERE v = a.vec])",
               {{"Alpha", "2", "1"},
                {"Beta", "2", "1"},
                {"Gamma", "2", "1"},
                {"Delta", "2", "1"}});
}

TEST_F(EmbeddingCollectTest, collectsEmbeddingsPerGroup) {
    expectRows("MATCH (d:Doc) WITH d.name STARTS WITH 'D' AS dee, collect(d.vec) AS vs, collect(DISTINCT d.vec) AS distinctVs RETURN dee, size(vs), size(distinctVs)",
               {{"false", "3", "1"},
                {"true", "1", "1"}});
}

TEST_F(EmbeddingCollectTest, unwindsCollectedEmbeddings) {
    expectRows("MATCH (d:Doc) WITH collect(d.vec) AS vs UNWIND vs AS v MATCH (a:Doc {name: 'Delta'}) WHERE v = a.vec RETURN count(*)",
               {{"1"}});
}

TEST_F(EmbeddingCollectTest, collectsDistinctListsOfEmbeddings) {
    expectRows("MATCH (d:Doc) WHERE d.vec IS NOT NULL RETURN size(collect([d.vec])), size(collect(DISTINCT [d.vec]))", {{"4", "2"}});
}

TEST_F(EmbeddingCollectTest, collectsDistinctEmbeddingsHoldingNaN) {
    expectRows("MATCH (d:Odd) RETURN size(collect(d.vec)), size(collect(DISTINCT d.vec))", {{"2", "1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
