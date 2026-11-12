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

constexpr std::string_view EMBEDDING_ONTO_INTEGER =
    "Property 'e' (Embedding) can not be filed under 'e (Embedding)': main holds 'e (Embedding)' as Int64";
constexpr std::string_view STRING_ONTO_INTEGER =
    "Property 'x' (String) can not be filed under 'x (String)': main holds 'x (String)' as Int64";
constexpr std::string_view TWO_EMBEDDINGS_INTO_ONE =
    "Properties 'e' (Embedding) and 'e (Embedding)' (Embedding) of the change can not both be filed under 'e (Embedding)'";
constexpr std::string_view EMBEDDING_AND_INTEGER_INTO_ONE =
    "Properties 'e' (Embedding) and 'e (Embedding)' (Int64) of the change can not both be filed under 'e (Embedding)'";

constexpr std::string_view READ_NODES =
    "MATCH (n:Vec) RETURN n.name, n.e, cosine_similarity(n.`e (Embedding)`, (1.0, 0.0))";
constexpr std::string_view READ_EDGES =
    "MATCH (s:Vec)-[r:LINK]->(:Vec) RETURN s.name, r.e, cosine_similarity(r.`e (Embedding)`, (1.0, 0.0))";
constexpr std::string_view READ_E_PROPERTY_TYPES =
    "CALL db.propertyTypes() YIELD propertyType, valueType "
    "WITH propertyType, valueType WHERE propertyType STARTS WITH 'e' "
    "RETURN propertyType, valueType";

constexpr std::string_view READ_CHANGE_NODES = "MATCH (n:Vec) RETURN n.name, cosine_similarity(n.e, (1.0, 0.0))";
constexpr std::string_view READ_CHANGE_EDGES =
    "MATCH (s:Vec)-[r:LINK]->(:Vec) RETURN s.name, cosine_similarity(r.e, (1.0, 0.0))";
constexpr std::string_view READ_CHANGE_TWO_EMBEDDINGS =
    "MATCH (n:Vec) RETURN n.name, cosine_similarity(n.e, (1.0, 0.0)), cosine_similarity(n.`e (Embedding)`, (1.0, 0.0))";

struct ChangeState {
    CommitHash base;
    Rows history;
    Rows propertyTypes;
};

}

class RenamedPropertyCollisionTest : public TuringTest {
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

    void readRows(std::string_view query, ChangeID change, Rows& rows) {
        QueryStatus status;
        StringRowSink sink;
        execute(query, change, status, sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        sink.sortedRows(rows);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        Rows rows;
        readRows(query, ChangeID::head(), rows);
        EXPECT_EQ(rows, expected) << query;
    }

    void mergeDataParts() {
        run("MERGE_DATAPARTS", ChangeID::head());
    }

    void readChangeState(ChangeID change, ChangeState& state) {
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const Graph* graph = system.getGraph(_graphName);
            const auto found = system.getChange(graph, change);
            ASSERT_TRUE(found);

            state.base = found.value()->baseHash();
        }

        readRows("CALL db.history() YIELD commit RETURN commit", change, state.history);
        readRows("CALL db.propertyTypes() YIELD propertyType, valueType RETURN propertyType, valueType", change, state.propertyTypes);
    }

    void expectSubmitRejected(ChangeID change,
                              std::string_view reason,
                              std::string_view entityQuery,
                              const Rows& entities) {
        ChangeState before;
        readChangeState(change, before);

        expectError("CHANGE SUBMIT", change, reason);

        ChangeState after;
        readChangeState(change, after);
        EXPECT_EQ(after.base, before.base);
        EXPECT_EQ(after.history, before.history);
        EXPECT_EQ(after.propertyTypes, before.propertyTypes);

        run("COMMIT", change);

        Rows rows;
        readRows(entityQuery, change, rows);
        EXPECT_EQ(rows, entities) << entityQuery;

        expectError("CHANGE SUBMIT", change, reason);
        run("CHANGE DELETE", change);
    }

    void openTwoChanges(ChangeID& first, ChangeID& second) {
        newChange(first);
        newChange(second);
    }

    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(RenamedPropertyCollisionTest, rejectsAPendingEmbeddingWhoseRenamedNameMainHoldsAsAnInteger) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', e: 1, `e (Embedding)`: 1})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", second);
    expectSubmitRejected(second, EMBEDDING_ONTO_INTEGER, READ_CHANGE_NODES, {{"b", "1"}});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e, n.`e (Embedding)`", {{"a", "1", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, rejectsACommittedEmbeddingWhoseRenamedNameMainHoldsAsAnInteger) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', e: 1, `e (Embedding)`: 1})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", second);
    run("COMMIT", second);
    expectSubmitRejected(second, EMBEDDING_ONTO_INTEGER, READ_CHANGE_NODES, {{"b", "1"}});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e, n.`e (Embedding)`", {{"a", "1", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, rejectsAPendingEdgeEmbeddingWhoseRenamedNameMainHoldsAsAnInteger) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a'})-[:LINK {e: 1, `e (Embedding)`: 1}]->(:Vec {name: 'a2'})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b'})-[:LINK {e: (1.0, 0.0)}]->(:Vec {name: 'b2'})", second);
    expectSubmitRejected(second, EMBEDDING_ONTO_INTEGER, READ_CHANGE_EDGES, {{"b", "1"}});

    expectRows("MATCH (s:Vec)-[r:LINK]->(:Vec) RETURN s.name, r.e, r.`e (Embedding)`", {{"a", "1", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, rejectsACommittedEdgeEmbeddingWhoseRenamedNameMainHoldsAsAnInteger) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a'})-[:LINK {e: 1, `e (Embedding)`: 1}]->(:Vec {name: 'a2'})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b'})-[:LINK {e: (1.0, 0.0)}]->(:Vec {name: 'b2'})", second);
    run("COMMIT", second);
    expectSubmitRejected(second, EMBEDDING_ONTO_INTEGER, READ_CHANGE_EDGES, {{"b", "1"}});

    expectRows("MATCH (s:Vec)-[r:LINK]->(:Vec) RETURN s.name, r.e, r.`e (Embedding)`", {{"a", "1", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, rejectsAPendingStringWhoseRenamedNameMainHoldsAsAnInteger) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', x: 1, `x (String)`: 1})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', x: 'b'})", second);
    expectSubmitRejected(second, STRING_ONTO_INTEGER, "MATCH (n:Vec) RETURN n.name, n.x", {{"b", "b"}});

    expectRows("MATCH (n:Vec) RETURN n.name, n.x, n.`x (String)`", {{"a", "1", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, rejectsACommittedStringWhoseRenamedNameMainHoldsAsAnInteger) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', x: 1, `x (String)`: 1})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', x: 'b'})", second);
    run("COMMIT", second);
    expectSubmitRejected(second, STRING_ONTO_INTEGER, "MATCH (n:Vec) RETURN n.name, n.x", {{"b", "b"}});

    expectRows("MATCH (n:Vec) RETURN n.name, n.x, n.`x (String)`", {{"a", "1", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, rejectsTwoPendingPropertiesOfAChangeThatTheRenameFilesUnderOneName) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', e: 1})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", second);
    run("CREATE (:Vec {name: 'c', `e (Embedding)`: (0.0, 1.0)})", second);
    expectSubmitRejected(second, TWO_EMBEDDINGS_INTO_ONE, READ_CHANGE_TWO_EMBEDDINGS, {{"b", "1", "null"}, {"c", "null", "0"}});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e", {{"a", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, rejectsTwoCommittedPropertiesOfAChangeThatTheRenameFilesUnderOneName) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', e: 1})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", second);
    run("COMMIT", second);
    run("CREATE (:Vec {name: 'c', `e (Embedding)`: (0.0, 1.0)})", second);
    run("COMMIT", second);
    expectSubmitRejected(second, TWO_EMBEDDINGS_INTO_ONE, READ_CHANGE_TWO_EMBEDDINGS, {{"b", "1", "null"}, {"c", "null", "0"}});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e", {{"a", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, rejectsTwoPropertiesOfAChangeThatTheRenameFilesUnderOneMainProperty) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', e: 1, `e (Embedding)`: (0.0, 1.0)})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", second);
    run("COMMIT", second);
    run("CREATE (:Vec {name: 'c', `e (Embedding)`: (1.0, 0.0)})", second);
    expectSubmitRejected(second, TWO_EMBEDDINGS_INTO_ONE, READ_CHANGE_TWO_EMBEDDINGS, {{"b", "1", "null"}, {"c", "null", "1"}});

    expectRows(READ_NODES, {{"a", "1", "0"}});
}

TEST_F(RenamedPropertyCollisionTest, rejectsARenameOntoAPropertyTheChangeHoldsWithAnotherType) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', e: 1})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", second);
    run("CREATE (:Vec {name: 'c', `e (Embedding)`: 2})", second);
    run("COMMIT", second);
    expectSubmitRejected(second,
                         EMBEDDING_AND_INTEGER_INTO_ONE,
                         "MATCH (n:Vec) RETURN n.name, cosine_similarity(n.e, (1.0, 0.0)), n.`e (Embedding)`",
                         {{"b", "1", "null"}, {"c", "null", "2"}});

    expectRows("MATCH (n:Vec) RETURN n.name, n.e", {{"a", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, renamesAPendingEmbeddingWhoseNameMainHoldsAsAnInteger) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', e: 1})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", second);
    run("CHANGE SUBMIT", second);

    expectRows(READ_NODES, {{"a", "1", "null"}, {"b", "null", "1"}});
    expectRows(READ_E_PROPERTY_TYPES, {{"e", "Int64"}, {"e (Embedding)", "Embedding"}});

    mergeDataParts();
    expectRows(READ_NODES, {{"a", "1", "null"}, {"b", "null", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, renamesACommittedEmbeddingWhoseNameMainHoldsAsAnInteger) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', e: 1})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", second);
    run("COMMIT", second);
    run("CHANGE SUBMIT", second);

    expectRows(READ_NODES, {{"a", "1", "null"}, {"b", "null", "1"}});
    expectRows(READ_E_PROPERTY_TYPES, {{"e", "Int64"}, {"e (Embedding)", "Embedding"}});

    mergeDataParts();
    expectRows(READ_NODES, {{"a", "1", "null"}, {"b", "null", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, renamesACommittedEdgeEmbeddingWhoseNameMainHoldsAsAnInteger) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a'})-[:LINK {e: 1}]->(:Vec {name: 'a2'})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b'})-[:LINK {e: (1.0, 0.0)}]->(:Vec {name: 'b2'})", second);
    run("COMMIT", second);
    run("CHANGE SUBMIT", second);

    expectRows(READ_EDGES, {{"a", "1", "null"}, {"b", "null", "1"}});

    mergeDataParts();
    expectRows(READ_EDGES, {{"a", "1", "null"}, {"b", "null", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, filesAPendingEmbeddingUnderTheRenamedPropertyMainHoldsAsAnEmbedding) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', e: 1, `e (Embedding)`: (0.0, 1.0)})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", second);
    run("CHANGE SUBMIT", second);

    expectRows(READ_NODES, {{"a", "1", "0"}, {"b", "null", "1"}});
    expectRows(READ_E_PROPERTY_TYPES, {{"e", "Int64"}, {"e (Embedding)", "Embedding"}});

    mergeDataParts();
    expectRows(READ_NODES, {{"a", "1", "0"}, {"b", "null", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, filesACommittedEmbeddingUnderTheRenamedPropertyMainHoldsAsAnEmbedding) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', e: 1, `e (Embedding)`: (0.0, 1.0)})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0)})", second);
    run("COMMIT", second);
    run("CHANGE SUBMIT", second);

    expectRows(READ_NODES, {{"a", "1", "0"}, {"b", "null", "1"}});
    expectRows(READ_E_PROPERTY_TYPES, {{"e", "Int64"}, {"e (Embedding)", "Embedding"}});

    mergeDataParts();
    expectRows(READ_NODES, {{"a", "1", "0"}, {"b", "null", "1"}});
}

TEST_F(RenamedPropertyCollisionTest, renamesTwoPropertiesWhoseNamesMainHoldsWithOtherTypes) {
    ChangeID first;
    ChangeID second;
    openTwoChanges(first, second);

    run("CREATE (:Vec {name: 'a', e: 1, `e (Embedding)`: (0.0, 1.0)})", first);
    run("CHANGE SUBMIT", first);

    run("CREATE (:Vec {name: 'b', e: (1.0, 0.0), `e (Embedding)`: 2})", second);
    run("COMMIT", second);
    run("CHANGE SUBMIT", second);

    expectRows("MATCH (n:Vec) RETURN n.name, n.e, n.`e (Embedding) (Int64)`, cosine_similarity(n.`e (Embedding)`, (1.0, 0.0))",
               {{"a", "1", "null", "0"}, {"b", "null", "2", "1"}});
    expectRows(READ_E_PROPERTY_TYPES, {{"e", "Int64"}, {"e (Embedding)", "Embedding"}, {"e (Embedding) (Int64)", "Int64"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
