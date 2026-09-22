#include <gtest/gtest.h>

#include <stddef.h>

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

namespace {

using Vector = std::vector<float>;

const Vector alphaVector {1.0f, 0.0f, 0.0f};
const Vector betaVector {0.0f, 1.0f, 0.0f};
const Vector linkVector {1.0f, 2.0f};

}

// SET of a null literal over an embedding property. The container holding one carries a
// dimension the null has to be written against, and the datapart a change builds knows
// none of its own, so the dimension is looked up in the dataparts behind it. simpledb
// carries no embedding, so the fixture is its own graph, in two dataparts:
//
//   Alpha, Beta (:Doc, vec)  and  Alpha -LINKS-> Beta (evec)   in the first
//   Plain (:Doc, no vec)                                       in the second
class SetEmbeddingNullTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildEmbeddingGraph(system.createGraph(_graphName));
    }

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        changeID = res.value()->id();
    }

    QueryStatus submit(const ChangeID& changeID) {
        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);

        return _env->getDB().query("CHANGE SUBMIT", submitState);
    }

    void applyWrite(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        NullSink sink;
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), changeID, &_env->getMem(), &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const QueryStatus submitStatus = submit(changeID);
        ASSERT_TRUE(submitStatus.isOk()) << "query: " << query << "\nerror: " << submitStatus.getError();
    }

    void expectWriteRejectedOnSubmit(std::string_view query, std::string_view reason) {
        ChangeID changeID;
        openChange(changeID);

        NullSink sink;
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), changeID, &_env->getMem(), &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const QueryStatus submitStatus = submit(changeID);
        ASSERT_FALSE(submitStatus.isOk()) << "submit accepted: " << query;

        const std::string& error = submitStatus.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << "query: " << query << "\nerror: " << error;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
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

        const auto vectorNode = [&](std::string_view name, const Vector& vector) -> NodeID {
            const NodeID node = writer.addNode({"Doc"});
            writer.addNodeProperty<types::String>(node, "name", types::String::Primitive {name});
            writer.addNodeProperty<types::Embedding>(node, "vec", types::Embedding::Primitive {vector});
            return node;
        };

        const NodeID alpha = vectorNode("Alpha", alphaVector);
        const NodeID beta = vectorNode("Beta", betaVector);

        const EdgeRecord link = writer.addEdge("LINKS", alpha, beta);
        writer.addEdgeProperty<types::String>(link, "name", types::String::Primitive {"Alpha -> Beta"});
        writer.addEdgeProperty<types::Embedding>(link, "evec", types::Embedding::Primitive {linkVector});

        writer.submit();

        const NodeID plain = writer.addNode({"Doc"});
        writer.addNodeProperty<types::String>(plain, "name", types::String::Primitive {"Plain"});

        writer.submit();

        jobSystem.terminate();
    }
};

TEST_F(SetEmbeddingNullTest, readsNullForTheEmbeddingSetToNullOnAMatchedNode) {
    applyWrite("MATCH (d:Doc {name: 'Alpha'}) SET d.vec = null");

    expectRows("MATCH (d:Doc) WHERE d.vec IS NULL RETURN d.name", {{"Alpha"}, {"Plain"}});
    expectRows("MATCH (d:Doc) WHERE d.vec IS NOT NULL RETURN d.name", {{"Beta"}});
}

// The similarity of a vector with itself is 1 on the two nodes that still hold one
TEST_F(SetEmbeddingNullTest, readsNullForTheSimilarityOfTheEmbeddingSetToNull) {
    expectRows("MATCH (d:Doc) RETURN d.name, cosine_similarity(d.vec, d.vec)",
               {{"Alpha", "1.000000"}, {"Beta", "1.000000"}, {"Plain", "null"}});

    applyWrite("MATCH (d:Doc {name: 'Alpha'}) SET d.vec = null");

    expectRows("MATCH (d:Doc) RETURN d.name, cosine_similarity(d.vec, d.vec)",
               {{"Alpha", "null"}, {"Beta", "1.000000"}, {"Plain", "null"}});
}

// Plain carries no vec and sits in the second datapart, so the dimension the null is
// written against is only found in the datapart behind it
TEST_F(SetEmbeddingNullTest, setsToNullAnEmbeddingTheMatchedNodeNeverCarried) {
    applyWrite("MATCH (d:Doc {name: 'Plain'}) SET d.vec = null");

    expectRows("MATCH (d:Doc) WHERE d.vec IS NOT NULL RETURN d.name", {{"Alpha"}, {"Beta"}});
}

TEST_F(SetEmbeddingNullTest, readsNullForTheEmbeddingSetToNullOnAMatchedEdge) {
    applyWrite("MATCH ()-[e:LINKS]->() SET e.evec = null");

    expectRows("MATCH ()-[e:LINKS]->() RETURN e.name, e.evec IS NULL", {{"Alpha -> Beta", "true"}});
}

// evec is an edge property, so no datapart holds a node container to take a dimension
// from. The write is accepted and the change fails to build at submit.
TEST_F(SetEmbeddingNullTest, rejectsTheNullWhenNoDatapartKnowsTheDimension) {
    expectWriteRejectedOnSubmit("MATCH (d:Doc {name: 'Alpha'}) SET d.evec = null",
                                "dimension is unknown");

    expectRows("MATCH (d:Doc) WHERE d.vec IS NOT NULL RETURN d.name", {{"Alpha"}, {"Beta"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
