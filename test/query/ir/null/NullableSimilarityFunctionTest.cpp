#include <gtest/gtest.h>

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

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

class NullableSimilarityFunctionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildDocumentGraph(system.createGraph(_graphName));
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

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

        const std::array<float, 4> along {1.0f, 0.0f, 0.0f, 0.0f};
        const std::array<float, 4> across {0.0f, 1.0f, 0.0f, 0.0f};

        const NodeID alongNode = writer.addNode({"Doc"});
        writer.addNodeProperty<types::String>(alongNode, "name", "along");
        writer.addNodeProperty<types::Embedding>(alongNode, "vec", std::span<const float> {along});

        const NodeID acrossNode = writer.addNode({"Doc"});
        writer.addNodeProperty<types::String>(acrossNode, "name", "across");
        writer.addNodeProperty<types::Embedding>(acrossNode, "vec", std::span<const float> {across});

        const NodeID emptyNode = writer.addNode({"Doc"});
        writer.addNodeProperty<types::String>(emptyNode, "name", "empty");

        writer.submit();
        jobSystem.terminate();
    }
};

TEST_F(NullableSimilarityFunctionTest, cosineSimilarityOfAMissingVectorIsNull) {
    expectRows("MATCH (d:Doc) RETURN d.name, cosine_similarity(d.vec, (1.0, 0.0, 0.0, 0.0))",
               {
                   {"across", "0"},
                   {"along", "1"},
                   {"empty", "null"},
               });
}

TEST_F(NullableSimilarityFunctionTest, euclideanDistanceOfAMissingVectorIsNull) {
    expectRows("MATCH (d:Doc) RETURN d.name, euclidean_distance(d.vec, (1.0, 0.0, 0.0, 0.0))",
               {
                   {"across", "1.4142135381698608"},
                   {"along", "0"},
                   {"empty", "null"},
               });
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
