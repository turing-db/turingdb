#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <span>
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

// The benchmark's query, verbatim but for the three parameters it is given: the country
// name and the two bounds a creationDate is compared against
std::string bi11Query(std::string_view country, int64_t startDate, int64_t endDate) {
    const std::string start = std::to_string(startDate);
    const std::string end = std::to_string(endDate);

    return "MATCH (a:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country:Country {name: '" + std::string(country) + "'}), "
           "      (a)-[k1:KNOWS]-(b:Person) "
           "WHERE a.id < b.id "
           "  AND " + start + " <= k1.creationDate AND k1.creationDate <= " + end + " "
           "WITH DISTINCT country, a, b "
           "MATCH (b)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country) "
           "WITH DISTINCT country, a, b "
           "MATCH (b)-[k2:KNOWS]-(c:Person), "
           "      (c)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country) "
           "WHERE b.id < c.id "
           "  AND " + start + " <= k2.creationDate AND k2.creationDate <= " + end + " "
           "WITH DISTINCT a, b, c "
           "MATCH (c)-[k3:KNOWS]-(a) "
           "WHERE " + start + " <= k3.creationDate AND k3.creationDate <= " + end + " "
           "WITH DISTINCT a, b, c "
           "RETURN count(*) AS count";
}

}

// LDBC SNB Business Intelligence query 11, "Friend triangles": five chained WITH
// barriers, the last of them joining two variables both bound. It runs as written under
// two substitutions the engine has no other spelling for: its parameters, and a
// creationDate stored as the epoch integer its datetime bounds are compared against.
class LdbcBi11Test : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildGraph(system.createGraph(_graphName));
    }

    // Three candidate triangles, one of them an answer: (1,2,3) closes inside India and
    // inside the date window, (1,2,4) reaches Paris, and (1,2,5) closes over an edge that
    // predates the window. Persons 1 and 2 know each other over an edge each way, so an
    // undirected hop matches that pair twice and every count depends on the DISTINCTs.
    void buildGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const NodeID india = writer.addNode({"Country"});
        writer.addNodeProperty<types::String>(india, "name", "India");

        const NodeID france = writer.addNode({"Country"});
        writer.addNodeProperty<types::String>(france, "name", "France");

        const NodeID delhi = writer.addNode({"City"});
        writer.addNodeProperty<types::String>(delhi, "name", "Delhi");

        const NodeID mumbai = writer.addNode({"City"});
        writer.addNodeProperty<types::String>(mumbai, "name", "Mumbai");

        const NodeID paris = writer.addNode({"City"});
        writer.addNodeProperty<types::String>(paris, "name", "Paris");

        writer.addEdge("IS_PART_OF", delhi, india);
        writer.addEdge("IS_PART_OF", mumbai, india);
        writer.addEdge("IS_PART_OF", paris, france);

        const std::vector<NodeID> homes {delhi, delhi, mumbai, paris, delhi};

        std::vector<NodeID> persons;
        for (size_t index = 0; index < homes.size(); index++) {
            const NodeID person = writer.addNode({"Person"});
            writer.addNodeProperty<types::Int64>(person, "id", static_cast<int64_t>(index + 1));
            writer.addEdge("IS_LOCATED_IN", person, homes[index]);
            persons.push_back(person);
        }

        const auto knows = [&](size_t from, size_t to, int64_t creationDate) {
            const EdgeRecord edge = writer.addEdge("KNOWS", persons[from - 1], persons[to - 1]);
            writer.addEdgeProperty<types::Int64>(edge, "creationDate", std::move(creationDate));
        };

        knows(1, 2, 100);
        knows(2, 1, 100); // the 1-2 pair twice, so the DISTINCTs have a duplicate to drop
        knows(2, 3, 100);
        knows(1, 3, 100);
        knows(1, 4, 100);
        knows(2, 4, 100);
        knows(1, 5, 100);
        knows(2, 5, 10);

        writer.submit();
        jobSystem.terminate();
    }

    void expectTriangleCount(std::string_view country,
                             int64_t startDate,
                             int64_t endDate,
                             uint64_t expected) {
        CountSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              bi11Query(country, startDate, endDate),
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << status.getError();

        ASSERT_EQ(sink.counts().size(), 1u);
        EXPECT_EQ(sink.counts()[0], expected);
    }

    const std::string _graphName = "ldbc";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Only (1, 2, 3) qualifies: (1, 2, 4) reaches Paris and the 2-5 edge predates the window
TEST_F(LdbcBi11Test, countsFriendTriangles) {
    expectTriangleCount("India", 50, 200, 1);
}

// Widening the window to admit the 2-5 edge admits the triangle it closes
TEST_F(LdbcBi11Test, countsTheTriangleAWiderWindowAdmits) {
    expectTriangleCount("India", 0, 200, 2);
}

// One Person lives in France, and a triangle takes three
TEST_F(LdbcBi11Test, countsNoTriangleInACountryWithOnePerson) {
    expectTriangleCount("France", 0, 200, 0);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
