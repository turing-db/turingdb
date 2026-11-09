#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"
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

constexpr std::string_view TWO_THEN_THREE = "Embedding property 'e' has dimension 2 and cannot take an embedding of dimension 3";
constexpr std::string_view THREE_THEN_TWO = "Embedding property 'e' has dimension 3 and cannot take an embedding of dimension 2";

}

class EmbeddingPropertyDimensionTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    void execute(std::string_view query, ChangeID change, const ParameterMap* parameters, QueryStatus& status, StringRowSink& sink) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), change, &sink, parameters);
    }

    void newChange(ChangeID& change) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto opened = system.newChange(_graphName);
        ASSERT_TRUE(opened);

        change = opened.value()->id();
    }

    void run(std::string_view query, ChangeID change, const ParameterMap* parameters = nullptr) {
        QueryStatus status;
        StringRowSink sink;
        execute(query, change, parameters, status, sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    void expectError(std::string_view query, ChangeID change, std::string_view reason) {
        QueryStatus status;
        StringRowSink sink;
        execute(query, change, nullptr, status, sink);
        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
        EXPECT_EQ(error.find("Internal Error"), std::string::npos) << query << ": " << error;
    }

    void runWrite(std::string_view query) {
        ChangeID change;
        newChange(change);
        run(query, change);
        run("CHANGE SUBMIT", change);
    }

    // The write itself runs; its change is refused at submit, then dropped so a merge can run
    void expectWriteRejected(std::string_view query, std::string_view reason, const ParameterMap* parameters = nullptr) {
        ChangeID change;
        newChange(change);
        run(query, change, parameters);
        expectError("CHANGE SUBMIT", change, reason);
        run("CHANGE DELETE", change);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        QueryStatus status;
        StringRowSink sink;
        execute(query, ChangeID::head(), nullptr, status, sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        Rows rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << query;
    }

    void mergeDataParts() {
        run("MERGE_DATAPARTS", ChangeID::head());
    }

    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(EmbeddingPropertyDimensionTest, rejectsTwoDimensionsInOneCreate) {
    expectWriteRejected("CREATE (:Vec {name: 'c', e: (1.0, 0.0)}), (:Vec {name: 'd', e: (1.0, 0.0, 0.0)})",
                        TWO_THEN_THREE);

    expectRows("MATCH (n:Vec) RETURN n.name", {});
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsAnotherDimensionInALaterChange) {
    runWrite("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})");
    expectWriteRejected("CREATE (:Vec {name: 'b', e: (1.0, 0.0, 0.0)})", TWO_THEN_THREE);

    mergeDataParts();
    expectRows("MATCH (n:Vec) RETURN n.name", {{"a"}});
    expectRows("MATCH (n:Vec) RETURN cosine_similarity(n.e, (1.0, 0.0))", {{"1"}});
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsASetOfAnotherDimension) {
    runWrite("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})");

    expectWriteRejected("MATCH (n:Vec {name: 'a'}) SET n.e = (1.0, 0.0, 0.0)", TWO_THEN_THREE);
    expectWriteRejected("MATCH (n:Person {name: 'Remy'}) SET n.e = (1.0, 0.0, 0.0)", TWO_THEN_THREE);
    expectWriteRejected("MATCH (n:Person {name: 'Remy'}) SET n += {e: (1.0, 0.0, 0.0)}", TWO_THEN_THREE);
    expectWriteRejected("MATCH (n:Person {name: 'Remy'}) SET n = {name: 'Remy', e: (1.0, 0.0, 0.0)}", TWO_THEN_THREE);
    expectWriteRejected("CREATE (n:Vec {name: 'b'}) SET n.e = (1.0, 0.0, 0.0)", TWO_THEN_THREE);

    mergeDataParts();
    expectRows("MATCH (n) WHERE n.e IS NOT NULL RETURN n.name", {{"a"}});
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsARowOfAnotherDimensionPastTheFirstChunk) {
    expectWriteRejected("UNWIND range(0, 70000) AS i "
                        "CREATE (:Vec {e: CASE WHEN i = 69999 THEN (1.0, 0.0, 0.0) ELSE (1.0, 0.0) END})",
                        TWO_THEN_THREE);
    expectWriteRejected("UNWIND [(1.0, 0.0), (1.0, 0.0, 0.0)] AS v MATCH (n:Person {name: 'Remy'}) SET n.e = v",
                        TWO_THEN_THREE);

    expectRows("MATCH (n) WHERE n.e IS NOT NULL RETURN n.name", {});
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsACreateBesideASetOfAnotherDimensionInOneChange) {
    ChangeID change;
    newChange(change);
    run("MATCH (n:Person {name: 'Remy'}) SET n.e = (1.0, 0.0)", change);
    run("CREATE (:Vec {name: 'c', e: (1.0, 0.0, 0.0)})", change);

    expectError("CHANGE SUBMIT", change, THREE_THEN_TWO);
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsAnEdgeEmbeddingOfAnotherDimension) {
    runWrite("CREATE (:Vec {name: 'a'})-[:LINK {e: (1.0, 0.0)}]->(:Vec {name: 'b'})");

    expectWriteRejected("MATCH (a:Vec {name: 'a'}), (b:Vec {name: 'b'}) CREATE (b)-[:LINK {e: (1.0, 0.0, 0.0)}]->(a)",
                        TWO_THEN_THREE);
    expectWriteRejected("MATCH ()-[r:LINK]->() SET r.e = (1.0, 0.0, 0.0)", TWO_THEN_THREE);

    mergeDataParts();
    expectRows("MATCH ()-[r:LINK]->() RETURN cosine_similarity(r.e, (1.0, 0.0))", {{"1"}});
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsANodeAndAnEdgeEmbeddingOfDifferentDimensions) {
    expectWriteRejected("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})-[:LINK {e: (1.0, 0.0, 0.0)}]->(:Vec {name: 'b'})",
                        TWO_THEN_THREE);

    runWrite("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})");
    expectWriteRejected("MATCH (a:Vec) CREATE (a)-[:LINK {e: (1.0, 0.0, 0.0)}]->(a)", TWO_THEN_THREE);
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsAMergeOfAnotherDimension) {
    runWrite("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})");

    expectWriteRejected("MERGE (n:Vec {name: 'y'}) ON CREATE SET n.e = (1.0, 0.0, 0.0)", TWO_THEN_THREE);
    expectWriteRejected("MERGE (n:Vec {name: 'a'}) ON MATCH SET n.e = (1.0, 0.0, 0.0)", TWO_THEN_THREE);

    runWrite("MERGE (n:Vec {name: 'z'}) ON CREATE SET n.e = (0.0, 1.0)");

    mergeDataParts();
    expectRows("MATCH (n:Vec) RETURN n.name", {{"a"}, {"z"}});
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsAParameterOfAnotherDimension) {
    runWrite("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})");

    ParameterMap parameters;
    parameters.insert("e").setEmbedding(std::vector<float> {1.0f, 0.0f, 0.0f});

    expectWriteRejected("CREATE (:Vec {name: 'b', e: $e})", TWO_THEN_THREE, &parameters);
    expectWriteRejected("MATCH (n:Vec {name: 'a'}) SET n.e = $e", TWO_THEN_THREE, &parameters);
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsTheSecondOfTwoChangesBranchedFromOneCommit) {
    ChangeID first;
    ChangeID second;
    newChange(first);
    newChange(second);

    run("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})", first);
    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0, 0.0)})", second);

    run("CHANGE SUBMIT", first);
    expectError("CHANGE SUBMIT", second, TWO_THEN_THREE);
    run("CHANGE DELETE", second);

    mergeDataParts();
    expectRows("MATCH (n:Vec) RETURN n.name", {{"a"}});
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsTheSecondOfTwoChangesOnceItCommitted) {
    ChangeID first;
    ChangeID second;
    newChange(first);
    newChange(second);

    run("CREATE (:Vec {name: 'a'})-[:LINK {e: (1.0, 0.0)}]->(:Vec {name: 'b'})", first);
    run("CREATE (:Vec {name: 'c', e: (1.0, 0.0, 0.0)})", second);
    run("COMMIT", second);

    run("CHANGE SUBMIT", first);
    expectError("CHANGE SUBMIT", second, TWO_THEN_THREE);
    run("CHANGE DELETE", second);

    mergeDataParts();
    expectRows("MATCH (n:Vec) RETURN n.name", {{"a"}, {"b"}});
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsACommitOfAnotherDimensionAndKeepsTheChange) {
    ChangeID change;
    newChange(change);

    run("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})", change);
    run("COMMIT", change);
    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0, 0.0)})", change);

    expectError("COMMIT", change, TWO_THEN_THREE);
    expectError("COMMIT", change, TWO_THEN_THREE);
    expectError("CHANGE SUBMIT", change, TWO_THEN_THREE);
    run("CHANGE DELETE", change);

    expectRows("MATCH (n:Vec) RETURN n.name", {});
}

TEST_F(EmbeddingPropertyDimensionTest, keepsOneDimensionThroughAMerge) {
    runWrite("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})");
    runWrite("MATCH (n:Person {name: 'Remy'}) SET n.e = (0.0, 1.0)");
    runWrite("MATCH (a:Vec), (b:Person {name: 'Remy'}) CREATE (a)-[:LINK {e: (1.0, 1.0)}]->(b)");

    mergeDataParts();
    expectRows("MATCH (n) WHERE n.e IS NOT NULL RETURN n.name, cosine_similarity(n.e, (1.0, 0.0))", {{"Remy", "0"}, {"a", "1"}});

    expectWriteRejected("MATCH (n:Person {name: 'Adam'}) SET n.e = (1.0, 0.0, 0.0)", TWO_THEN_THREE);
}

TEST_F(EmbeddingPropertyDimensionTest, holdsEmbeddingsOfAnyDimensionInAListOrAMap) {
    runWrite("CREATE (:Vec {name: 'a', l: [(1.0, 0.0), (1.0, 0.0, 0.0)], m: {two: (1.0, 0.0), three: (1.0, 0.0, 0.0)}})");
    runWrite("CREATE (:Vec {name: 'b', l: [(1.0, 0.0, 0.0, 0.0)], m: {four: (1.0, 0.0, 0.0, 0.0)}})");

    mergeDataParts();
    expectRows("MATCH (n:Vec) RETURN n.name, size(n.l), size(keys(n.m))", {{"a", "2", "2"}, {"b", "1", "1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
