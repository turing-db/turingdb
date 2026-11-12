#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

constexpr std::string_view RENAMED_TWO_THEN_THREE =
    "Embedding property 'e (Embedding)' has dimension 2 and cannot take an embedding of dimension 3";
constexpr std::string_view TWO_THEN_THREE = "Embedding property 'e' has dimension 2 and cannot take an embedding of dimension 3";

}

class EmbeddingDimensionRebasedPropertyTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    void execute(std::string_view query, ChangeID change, QueryStatus& status, StringRowSink& sink) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), change, &sink, nullptr);
    }

    void newChange(ChangeID& change) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto opened = system.newChange(_graphName);
        ASSERT_TRUE(opened);

        change = opened.value()->id();
    }

    void run(std::string_view query, ChangeID change) {
        QueryStatus status;
        StringRowSink sink;
        execute(query, change, status, sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    void expectError(std::string_view query, ChangeID change, std::string_view reason) {
        QueryStatus status;
        StringRowSink sink;
        execute(query, change, status, sink);
        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
        EXPECT_EQ(error.find("Internal Error"), std::string::npos) << query << ": " << error;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        QueryStatus status;
        StringRowSink sink;
        execute(query, ChangeID::head(), status, sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        Rows rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << query;
    }

    void mergeDataParts() {
        run("MERGE_DATAPARTS", ChangeID::head());
    }

    // Three changes branch from one commit. The first gives main an integer 'e', so the
    // rebase of the second renames its embedding 'e' to 'e (Embedding)'
    void submitIntegerThenRenamedEmbedding(ChangeID& third) {
        ChangeID first;
        ChangeID second;
        newChange(first);
        newChange(second);
        newChange(third);

        run("CREATE (:Vec {name: 'a', e: 1})", first);
        run("CHANGE SUBMIT", first);

        run("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", second);
        run("COMMIT", second);
        run("CHANGE SUBMIT", second);
    }

    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(EmbeddingDimensionRebasedPropertyTest, rejectsACommittedEmbeddingOfAnotherDimensionThanTheRenamedProperty) {
    ChangeID third;
    submitIntegerThenRenamedEmbedding(third);

    run("CREATE (:Vec {name: 'c', e: (1.0, 0.0, 0.0)})", third);
    run("COMMIT", third);
    expectError("CHANGE SUBMIT", third, RENAMED_TWO_THEN_THREE);
    run("CHANGE DELETE", third);

    mergeDataParts();
    expectRows("MATCH (n:Vec) RETURN n.name, n.e, cosine_similarity(n.`e (Embedding)`, (1.0, 0.0))",
               {{"a", "1", "null"}, {"b", "null", "1"}});
}

TEST_F(EmbeddingDimensionRebasedPropertyTest, rejectsAPendingEmbeddingOfAnotherDimensionThanTheRenamedProperty) {
    ChangeID third;
    submitIntegerThenRenamedEmbedding(third);

    run("CREATE (:Vec {name: 'c', e: (1.0, 0.0, 0.0)})", third);
    expectError("CHANGE SUBMIT", third, RENAMED_TWO_THEN_THREE);
    run("CHANGE DELETE", third);

    mergeDataParts();
    expectRows("MATCH (n:Vec) RETURN n.name", {{"a"}, {"b"}});
}

TEST_F(EmbeddingDimensionRebasedPropertyTest, rejectsAnEdgeEmbeddingOfAnotherDimensionThanTheRenamedProperty) {
    ChangeID third;
    submitIntegerThenRenamedEmbedding(third);

    run("CREATE (:Vec {name: 'c'})-[:LINK {e: (1.0, 0.0, 0.0)}]->(:Vec {name: 'd'})", third);
    run("COMMIT", third);
    expectError("CHANGE SUBMIT", third, RENAMED_TWO_THEN_THREE);
    run("CHANGE DELETE", third);

    mergeDataParts();
    expectRows("MATCH (n:Vec) RETURN n.name", {{"a"}, {"b"}});
}

TEST_F(EmbeddingDimensionRebasedPropertyTest, rejectsTwoPropertiesOfAChangeThatTheRebaseRenamesIntoOne) {
    ChangeID first;
    ChangeID second;
    newChange(first);
    newChange(second);

    run("CREATE (:Vec {name: 'a', e: 1})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", second);
    run("COMMIT", second);
    run("CREATE (:Vec {name: 'c', `e (Embedding)`: (1.0, 0.0, 0.0)})", second);
    run("COMMIT", second);
    expectError("CHANGE SUBMIT", second, RENAMED_TWO_THEN_THREE);
    run("CHANGE DELETE", second);

    mergeDataParts();
    expectRows("MATCH (n:Vec) RETURN n.name", {{"a"}});
}

TEST_F(EmbeddingDimensionRebasedPropertyTest, rejectsAnEmbeddingWhosePropertyMainNumbersApart) {
    ChangeID first;
    ChangeID second;
    newChange(first);
    newChange(second);

    run("CREATE (:Vec {name: 'a', x: 1, e: (1.0, 0.0)})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0, 0.0)})", second);
    run("COMMIT", second);
    expectError("CHANGE SUBMIT", second, TWO_THEN_THREE);
    run("CHANGE DELETE", second);

    mergeDataParts();
    expectRows("MATCH (n:Vec) RETURN n.name, cosine_similarity(n.e, (1.0, 0.0))", {{"a", "1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
