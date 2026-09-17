#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
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

// The benchmark's query, under the substitutions listed on the fixture below
std::string bi16Query(std::string_view tagA,
                      int64_t dateA,
                      std::string_view tagB,
                      int64_t dateB,
                      int64_t maxKnowsLimit) {
    return "UNWIND [['A', '" + std::string(tagA) + "', " + std::to_string(dateA) + "], "
           "        ['B', '" + std::string(tagB) + "', " + std::to_string(dateB) + "]] AS param "
           "WITH param[0] AS paramLetter, param[1] AS paramTagX, param[2] AS paramDateX "
           "CALL { "
           "  WITH paramTagX, paramDateX "
           "  MATCH (person1:Person)<-[:HAS_CREATOR]-(message1:Message)-[:HAS_TAG]->(tag:Tag {name: paramTagX}) "
           "  WHERE message1.creationDate = paramDateX "
           "  OPTIONAL MATCH (person1)-[:KNOWS]-(person2:Person)<-[:HAS_CREATOR]-(message2:Message)-[:HAS_TAG]->(tag) "
           "  WHERE message2.creationDate = paramDateX "
           "  WITH person1, count(DISTINCT message1) AS cm, count(DISTINCT person2) AS cp2 "
           "  WHERE cp2 <= " + std::to_string(maxKnowsLimit) + " "
           "  RETURN person1, cm "
           "} "
           "WITH person1, collect([paramLetter, cm]) AS results "
           "WHERE size(results) = 2 "
           "RETURN person1.id, "
           "       results[0][1] AS messageCountA, "
           "       results[1][1] AS messageCountB "
           "ORDER BY messageCountA + messageCountB DESC, person1.id ASC "
           "LIMIT 20";
}

// The letters of the two collected entries, which is what makes results[0] the A one
std::string collectOrderQuery() {
    return "UNWIND [['A', 'Meryl_Streep', 100], ['B', 'Hank_Williams', 200]] AS param "
           "WITH param[0] AS paramLetter, param[1] AS paramTagX, param[2] AS paramDateX "
           "CALL { "
           "  WITH paramTagX, paramDateX "
           "  MATCH (person1:Person)<-[:HAS_CREATOR]-(message1:Message)-[:HAS_TAG]->(tag:Tag {name: paramTagX}) "
           "  WHERE message1.creationDate = paramDateX "
           "  RETURN person1, count(DISTINCT message1) AS cm "
           "} "
           "WITH person1, collect([paramLetter, cm]) AS results "
           "WHERE size(results) = 2 "
           "RETURN person1.id, results[0][0] AS firstLetter, results[1][0] AS secondLetter "
           "ORDER BY person1.id ASC";
}

}

// LDBC SNB Business Intelligence query 16, "Fake news detection": a CALL subquery run once
// per parameter row, whose body matches, joins an OPTIONAL MATCH onto what it matched,
// reduces the two into a grouped count each and filters on one of them.
//
// It runs as written under four substitutions the engine has no other spelling for. Its
// parameters are inlined, as in LdbcBi11Test. A creationDate is the epoch integer its
// datetime is compared against, so date(x) = date(y) is x = y. Its parameter rows are
// lists rather than maps and their fields are read by index, since there is no map type.
// Its two answers are read off the collected list by index rather than by a comprehension
// over the letter, which is what collectsTheParameterRowsInUnwindOrder pins.
class LdbcBi16Test : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildGraph(system.createGraph(_graphName));
    }

    // Four Persons under two tags. Person 1 and Person 4 post under both, so they are the
    // answer; Person 2 posts under one; Person 3 posts under both but knows two Persons
    // who posted the same tag that day, which is what maxKnowsLimit cuts. Person 4 has a
    // further message under the first tag on another day, which no answer counts.
    void buildGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const NodeID tagA = writer.addNode({"Tag"});
        writer.addNodeProperty<types::String>(tagA, "name", "Meryl_Streep");

        const NodeID tagB = writer.addNode({"Tag"});
        writer.addNodeProperty<types::String>(tagB, "name", "Hank_Williams");

        std::vector<NodeID> persons;
        for (int64_t id = 1; id <= 4; id++) {
            const NodeID person = writer.addNode({"Person"});
            writer.addNodeProperty<types::Int64>(person, "id", std::move(id));
            persons.push_back(person);
        }

        const auto message = [&](size_t creator, NodeID tag, int64_t creationDate) {
            const NodeID node = writer.addNode({"Message"});
            writer.addNodeProperty<types::Int64>(node, "creationDate", std::move(creationDate));
            writer.addEdge("HAS_CREATOR", node, persons[creator - 1]);
            writer.addEdge("HAS_TAG", node, tag);
        };

        message(1, tagA, 100);
        message(1, tagA, 100);
        message(1, tagB, 200);

        message(2, tagA, 100);

        message(3, tagA, 100);
        message(3, tagB, 200);

        message(4, tagA, 100);
        message(4, tagB, 200);
        message(4, tagA, 999);

        writer.addEdge("KNOWS", persons[0], persons[2]);
        writer.addEdge("KNOWS", persons[1], persons[2]);

        writer.submit();
        jobSystem.terminate();
    }

    void expectRowsInOrder(const std::string& query, const Rows& expected) {
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

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "ldbc";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Person 1 posted twice under the first tag that day and once under the second, Person 4
// once under each. Person 2 is missing from the second tag and Person 3 from the first,
// so neither collects the two entries the query keeps.
TEST_F(LdbcBi16Test, findsThePersonsPostingUnderBothTags) {
    expectRowsInOrder(bi16Query("Meryl_Streep", 100, "Hank_Williams", 200, 1),
                      {{"1", "2", "1"}, {"4", "1", "1"}});
}

// The benchmark's own note, that too high a maxKnowsLimit returns a false positive: Person
// 3 knows two Persons who posted the first tag that day, and a limit of two admits them
TEST_F(LdbcBi16Test, aHigherKnowsLimitAdmitsThePersonItWasCutting) {
    expectRowsInOrder(bi16Query("Meryl_Streep", 100, "Hank_Williams", 200, 2),
                      {{"1", "2", "1"}, {"3", "1", "1"}, {"4", "1", "1"}});
}

// Only Person 4 posted the first tag on that day, and they posted the second tag too
TEST_F(LdbcBi16Test, countsTheMessagesOfTheGivenDayAlone) {
    expectRowsInOrder(bi16Query("Meryl_Streep", 999, "Hank_Williams", 200, 1),
                      {{"4", "1", "1"}});
}

// A tag nobody posted leaves every Person one entry short
TEST_F(LdbcBi16Test, reportsNoOneWhenATagIsUnused) {
    expectRowsInOrder(bi16Query("Meryl_Streep", 100, "Jeff_Bridges", 200, 1), {});
}

// The rows reach the collect in the order the UNWIND lists the parameters, which is what
// lets the answers be read off by index in place of the benchmark's list comprehension
TEST_F(LdbcBi16Test, collectsTheParameterRowsInUnwindOrder) {
    expectRowsInOrder(collectOrderQuery(), {{"1", "A", "B"}, {"3", "A", "B"}, {"4", "A", "B"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
