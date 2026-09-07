#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// The benchmark's query, verbatim but for the message id it is given and its CASE, which
// the parser rejects as unimplemented. CASE r WHEN null THEN false ELSE true END asks
// whether the OPTIONAL MATCH bound r, which is what r IS NOT NULL asks.
std::string is7Query(int64_t messageId) {
    return "MATCH (m:Message {id: " + std::to_string(messageId) + " })<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person) "
           "    OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p) "
           "    RETURN c.id AS commentId, "
           "        c.content AS commentContent, "
           "        c.creationDate AS commentCreationDate, "
           "        p.id AS replyAuthorId, "
           "        p.firstName AS replyAuthorFirstName, "
           "        p.lastName AS replyAuthorLastName, "
           "        r IS NOT NULL AS replyAuthorKnowsOriginalMessageAuthor "
           "    ORDER BY commentCreationDate DESC, replyAuthorId";
}

}

// LDBC SNB Interactive short read 7, "Replies of a message". The only query of the
// benchmark whose OPTIONAL MATCH the engine can run: the other three reach for
// variable-length paths, shortestPath, pattern predicates, size() or datetime().
//
// Its optional pattern closes onto two variables the mandatory match already bound - m and
// p - so a reply whose author does not know the original author keeps its row with r null.
class LdbcIs7Test : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildGraph(system.createGraph(_graphName));
    }

    // Alice posts message 100. Bob, Carol and Dan reply to it; Alice knows Bob alone, so
    // Bob's row is the one the optional pattern matches. Erin replies to another message,
    // which no row of the answer may reach.
    void buildGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const auto addPerson = [&](int64_t id, std::string_view firstName, std::string_view lastName) {
            const NodeID person = writer.addNode({"Person"});
            writer.addNodeProperty<types::Int64>(person, "id", std::move(id));
            writer.addNodeProperty<types::String>(person, "firstName", std::string_view(firstName));
            writer.addNodeProperty<types::String>(person, "lastName", std::string_view(lastName));
            return person;
        };

        const NodeID alice = addPerson(1, "Alice", "Adams");
        const NodeID bob = addPerson(2, "Bob", "Brown");
        const NodeID carol = addPerson(3, "Carol", "Clark");
        const NodeID dan = addPerson(4, "Dan", "Davis");
        const NodeID erin = addPerson(5, "Erin", "Evans");

        const auto addMessage = [&](std::string_view kind, int64_t id, std::string_view content, int64_t creationDate) {
            const NodeID message = writer.addNode({"Message", std::string(kind)});
            writer.addNodeProperty<types::Int64>(message, "id", std::move(id));
            writer.addNodeProperty<types::String>(message, "content", std::string_view(content));
            writer.addNodeProperty<types::Int64>(message, "creationDate", std::move(creationDate));
            return message;
        };

        const NodeID post = addMessage("Post", 100, "the original post", 1000);
        writer.addEdge("HAS_CREATOR", post, alice);

        const NodeID otherPost = addMessage("Post", 101, "an unrelated post", 1000);
        writer.addEdge("HAS_CREATOR", otherPost, alice);

        const auto reply = [&](int64_t id, std::string_view content, int64_t creationDate, NodeID author, NodeID target) {
            const NodeID comment = addMessage("Comment", id, content, creationDate);
            writer.addEdge("REPLY_OF", comment, target);
            writer.addEdge("HAS_CREATOR", comment, author);
        };

        reply(201, "bob replies", 3000, bob, post);
        reply(202, "carol replies", 2000, carol, post);
        reply(203, "dan replies", 2000, dan, post);
        reply(204, "erin replies elsewhere", 4000, erin, otherPost);

        // KNOWS is symmetric in the benchmark and the query hops it undirected, so the one
        // stored direction is matched from either end
        writer.addEdge("KNOWS", alice, bob);
        writer.addEdge("KNOWS", carol, dan);

        writer.submit();
        jobSystem.terminate();
    }

    void expectRowsInOrder(int64_t messageId, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              is7Query(messageId),
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << status.getError();

        EXPECT_EQ(sink.rows(), expected);
    }

    const std::string _graphName = "ldbc";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Bob knows Alice, so his row is the one the optional pattern matched; Carol and Dan know
// each other but not Alice, and their rows come back with r null
TEST_F(LdbcIs7Test, reportsWhichReplyAuthorsKnowTheMessageAuthor) {
    expectRowsInOrder(100,
                      {{"201", "bob replies", "3000", "2", "Bob", "Brown", "true"},
                       {"202", "carol replies", "2000", "3", "Carol", "Clark", "false"},
                       {"203", "dan replies", "2000", "4", "Dan", "Davis", "false"}});
}

// Only the replies of the message asked for: Erin's is the one reply of 101, and the three
// replies of 100 are no rows of this answer
TEST_F(LdbcIs7Test, reportsTheRepliesOfTheMessageAskedForAlone) {
    expectRowsInOrder(101, {{"204", "erin replies elsewhere", "4000", "5", "Erin", "Evans", "false"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
