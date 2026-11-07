#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryConfig.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "ID.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "JobSystem.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A MERGE pattern constrained to an embedding matches a node holding equal floats. Alpha
// and Beta carry one vector, Gamma the same floats with the sign of a zero flipped, Delta
// another vector, and Plain none.
class EmbeddingMergeKeyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildEmbeddingGraph(system.createGraph(_graphName));
    }

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto change = system.newChange(_graphName);
        ASSERT_TRUE(change);

        changeID = change.value()->id();
    }

    void expectWriteRejected(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        NullSink sink;
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), changeID, &sink);
        EXPECT_FALSE(status.isOk()) << "query accepted: " << query;
    }

    void applyWrite(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        NullSink sink;
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), changeID, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_env->getCompilerContext(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "error: " << submitStatus.getError();
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
    QueryConfig _queryConfig;

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

TEST_F(EmbeddingMergeKeyTest, mergesOneNodePerDistinctEmbedding) {
    applyWrite("MATCH (d:Doc) WHERE d.vec IS NOT NULL MERGE (:Copy {vec: d.vec})");

    expectRows("MATCH (c:Copy) RETURN count(*)", {{"2"}});

    applyWrite("MATCH (d:Doc) WHERE d.vec IS NOT NULL MERGE (:Copy {vec: d.vec})");

    expectRows("MATCH (c:Copy) RETURN count(*)", {{"2"}});
}

// The tuple literal is a constant embedding, keyed as the vectors Alpha, Beta and Gamma hold
TEST_F(EmbeddingMergeKeyTest, mergesOntoTheNodeHoldingAConstantEmbedding) {
    applyWrite("MATCH (d:Doc) WHERE d.vec IS NOT NULL MERGE (:Copy {vec: d.vec})");
    applyWrite("MERGE (:Copy {vec: (1.0, 0.0)})");
    applyWrite("MERGE (:Copy {vec: (1.0, -0.0)})");

    expectRows("MATCH (c:Copy) RETURN count(*)", {{"2"}});

    applyWrite("MERGE (:Copy {vec: (3.0, 4.0)})");
    applyWrite("MERGE (:Copy {vec: (3.0, 4.0)})");

    expectRows("MATCH (c:Copy) RETURN count(*)", {{"3"}});
}

TEST_F(EmbeddingMergeKeyTest, rejectsAConstantEmbeddingForAPropertyOfAnotherType) {
    expectWriteRejected("MERGE (:Doc {name: (1.0, 0.0)})");

    expectRows("MATCH (d:Doc) RETURN count(*)", {{"5"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
