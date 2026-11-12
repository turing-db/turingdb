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

constexpr std::string_view TWO_THEN_THREE = "Embedding property 'e' has dimension 2 and cannot take an embedding of dimension 3";
constexpr std::string_view THREE_THEN_TWO = "Embedding property 'e' has dimension 3 and cannot take an embedding of dimension 2";

}

class EmbeddingDimensionKeptTest : public TuringTest {
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

    void runWrite(std::string_view query) {
        ChangeID change;
        newChange(change);
        run(query, change);
        run("CHANGE SUBMIT", change);
    }

    void expectWriteRejected(std::string_view query, std::string_view reason) {
        ChangeID change;
        newChange(change);
        run(query, change);
        expectError("CHANGE SUBMIT", change, reason);
        run("CHANGE DELETE", change);
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

    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(EmbeddingDimensionKeptTest, keepsTheDimensionOnceEveryValueIsDeletedAndMerged) {
    runWrite("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})");
    runWrite("MATCH (n:Vec {name: 'a'}) DELETE n");
    expectWriteRejected("CREATE (:Vec {name: 'b', e: (1.0, 0.0, 0.0)})", TWO_THEN_THREE);

    mergeDataParts();
    expectWriteRejected("CREATE (:Vec {name: 'b', e: (1.0, 0.0, 0.0)})", TWO_THEN_THREE);
    expectWriteRejected("MATCH (n:Person {name: 'Remy'}) SET n.e = (1.0, 0.0, 0.0)", TWO_THEN_THREE);

    runWrite("CREATE (:Vec {name: 'c', e: (0.0, 1.0)})");
    expectRows("MATCH (n:Vec) RETURN n.name, cosine_similarity(n.e, (0.0, 1.0))", {{"c", "1"}});
}

TEST_F(EmbeddingDimensionKeptTest, keepsTheDimensionOfAnEdgePropertyOnceEveryValueIsDeletedAndMerged) {
    runWrite("CREATE (:Vec {name: 'a'})-[:LINK {e: (1.0, 0.0)}]->(:Vec {name: 'b'})");
    runWrite("MATCH ()-[r:LINK]->() DELETE r");
    mergeDataParts();

    expectWriteRejected("MATCH (a:Vec {name: 'a'}), (b:Vec {name: 'b'}) CREATE (a)-[:LINK {e: (1.0, 0.0, 0.0)}]->(b)",
                        TWO_THEN_THREE);
    expectWriteRejected("MATCH (a:Vec {name: 'a'}) SET a.e = (1.0, 0.0, 0.0)", TWO_THEN_THREE);

    expectRows("MATCH ()-[r:LINK]->() RETURN count(r)", {{"0"}});
}

TEST_F(EmbeddingDimensionKeptTest, rejectsChangesRebasedOnceEveryValueIsDeletedAndMerged) {
    runWrite("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})");
    runWrite("MATCH (n:Vec {name: 'a'}) DELETE n");
    mergeDataParts();

    ChangeID first;
    ChangeID pending;
    ChangeID committed;
    newChange(first);
    newChange(pending);
    newChange(committed);

    run("CREATE (:Vec {name: 'b'})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'c', e: (1.0, 0.0, 0.0)})", pending);
    expectError("CHANGE SUBMIT", pending, TWO_THEN_THREE);
    run("CHANGE DELETE", pending);

    run("CREATE (:Vec {name: 'd', e: (1.0, 0.0, 0.0)})", committed);
    expectError("COMMIT", committed, TWO_THEN_THREE);
    expectError("CHANGE SUBMIT", committed, TWO_THEN_THREE);
    run("CHANGE DELETE", committed);

    expectRows("MATCH (n:Vec) RETURN n.name", {{"b"}});
}

TEST_F(EmbeddingDimensionKeptTest, rejectsTheSecondOfTwoChangesThatEachCommittedAFirstDimension) {
    ChangeID first;
    ChangeID second;
    newChange(first);
    newChange(second);

    run("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})", first);
    run("COMMIT", first);
    run("CREATE (:Vec {name: 'b'})-[:LINK {e: (1.0, 0.0, 0.0)}]->(:Vec {name: 'c'})", second);
    run("COMMIT", second);

    run("CHANGE SUBMIT", first);
    expectError("CHANGE SUBMIT", second, TWO_THEN_THREE);
    run("CHANGE DELETE", second);

    runWrite("MATCH (n:Vec {name: 'a'}) DELETE n");
    mergeDataParts();
    expectWriteRejected("CREATE (:Vec {name: 'd', e: (1.0, 0.0, 0.0)})", TWO_THEN_THREE);
}

TEST_F(EmbeddingDimensionKeptTest, takesTheDimensionOfTheFirstValueWrittenAfterANull) {
    runWrite("CREATE (n:Vec {name: 'a', e: (1.0, 0.0)}) SET n.e = null");
    mergeDataParts();

    runWrite("MATCH (n:Vec {name: 'a'}) SET n.e = (1.0, 0.0, 0.0)");
    runWrite("MATCH (n:Vec {name: 'a'}) DELETE n");
    mergeDataParts();

    expectWriteRejected("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", THREE_THEN_TWO);
    runWrite("CREATE (:Vec {name: 'c', e: (0.0, 0.0, 1.0)})");
    expectRows("MATCH (n:Vec) RETURN n.name, cosine_similarity(n.e, (0.0, 0.0, 1.0))", {{"c", "1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
