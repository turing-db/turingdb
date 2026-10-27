#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
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

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

std::string_view firstSourceOf(std::string_view program) {
    const size_t scan = program.find("db.scan");
    const size_t fetch = program.find("db.fetch_nodes");
    const size_t source = std::min(scan, fetch);
    if (source == std::string_view::npos) {
        return {};
    }

    const size_t lineStart = program.rfind('\n', source) + 1;
    const size_t lineEnd = program.find('\n', source);

    return program.substr(lineStart, lineEnd - lineStart);
}

// Acme is controlled by Alice, Bob and Frank, who also control Globex, Initech and Wonka
const Rows sharingAcmesControllers {
    {"Globex", "Alice"},
    {"Globex", "Frank"},
    {"Initech", "Bob"},
    {"Wonka", "Frank"},
};

// Alice controls Acme, with Bob and Frank, and Globex, with Carol and Frank
const Rows coControllingWithAlice {
    {"Bob", "Acme"},
    {"Carol", "Globex"},
    {"Frank", "Acme"},
    {"Frank", "Globex"},
};

}

// Every query names one person, company or country, and is written to start from a node
// that every Person or every Company matches.
class RerootControlGraphTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildGraph(system.createGraph(_graphName));
    }

    void buildGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const auto addNamed = [&writer](std::string_view label, std::string_view name) {
            const NodeID node = writer.addNode({label});
            writer.addNodeProperty<types::String>(node, "name", std::string(name));
            return node;
        };

        const NodeID alice = addNamed("Person", "Alice");
        const NodeID bob = addNamed("Person", "Bob");
        const NodeID carol = addNamed("Person", "Carol");
        const NodeID dan = addNamed("Person", "Dan");
        const NodeID erin = addNamed("Person", "Erin");
        const NodeID frank = addNamed("Person", "Frank");

        const NodeID acme = addNamed("Company", "Acme");
        const NodeID globex = addNamed("Company", "Globex");
        const NodeID initech = addNamed("Company", "Initech");
        const NodeID umbrella = addNamed("Company", "Umbrella");
        const NodeID hooli = addNamed("Company", "Hooli");
        const NodeID stark = addNamed("Company", "Stark");
        const NodeID wayne = addNamed("Company", "Wayne");
        const NodeID wonka = addNamed("Company", "Wonka");

        const NodeID panama = addNamed("Country", "Panama");
        const NodeID cyprus = addNamed("Country", "Cyprus");
        const NodeID delaware = addNamed("Country", "Delaware");

        writer.addEdge("CONTROLS", alice, acme);
        writer.addEdge("CONTROLS", alice, globex);
        writer.addEdge("CONTROLS", bob, acme);
        writer.addEdge("CONTROLS", bob, initech);
        writer.addEdge("CONTROLS", carol, globex);
        writer.addEdge("CONTROLS", carol, hooli);
        writer.addEdge("CONTROLS", dan, initech);
        writer.addEdge("CONTROLS", dan, umbrella);
        writer.addEdge("CONTROLS", erin, stark);
        writer.addEdge("CONTROLS", erin, wayne);
        writer.addEdge("CONTROLS", frank, acme);
        writer.addEdge("CONTROLS", frank, globex);
        writer.addEdge("CONTROLS", frank, wonka);

        writer.addEdge("CONTROLS", globex, stark);
        writer.addEdge("CONTROLS", initech, wonka);
        writer.addEdge("CONTROLS", acme, umbrella);

        writer.addEdge("REGISTERED_IN", acme, delaware);
        writer.addEdge("REGISTERED_IN", globex, panama);
        writer.addEdge("REGISTERED_IN", initech, delaware);
        writer.addEdge("REGISTERED_IN", umbrella, cyprus);
        writer.addEdge("REGISTERED_IN", hooli, panama);
        writer.addEdge("REGISTERED_IN", stark, panama);
        writer.addEdge("REGISTERED_IN", wayne, cyprus);
        writer.addEdge("REGISTERED_IN", wonka, cyprus);

        writer.submit();
        jobSystem.terminate();
    }

    void runQuery(std::string_view query, StringRowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void dbProgramOf(std::string_view query, std::string& program) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), sink);

        ASSERT_EQ(sink.getRows().size(), 1u);
        program = sink.getRows().front().back();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectWalkedFrom(std::string_view query, const std::vector<std::string_view>& firstSource, const Rows& expected) {
        std::string program;
        dbProgramOf(query, program);

        const std::string_view source = firstSourceOf(program);
        for (const std::string_view part : firstSource) {
            EXPECT_TRUE(contains(source, part)) << part << " in " << program;
        }

        EXPECT_FALSE(contains(program, "db.fetch_nodes")) << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << program;
        EXPECT_FALSE(contains(program, "db.check_edge_type_constraint")) << program;

        expectRows(query, expected);
    }

    const std::string _graphName = "control";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(RerootControlGraphTest, findsCompaniesSharingAControllerFromTheNamedOne) {
    expectWalkedFrom("MATCH (c:Company)<-[:CONTROLS]-(p:Person)-[:CONTROLS]->(acme:Company {name: 'Acme'}) "
                     "WHERE c <> acme RETURN c.name, p.name",
                     {"db.scan_nodes_by_property_value", "Acme"},
                     sharingAcmesControllers);
    expectWalkedFrom("MATCH (c:Company)<-[:CONTROLS]-(p:Person)-[:CONTROLS]->(acme:Company) "
                     "WHERE acme.name = 'Acme' AND c <> acme RETURN c.name, p.name",
                     {"db.scan_nodes_by_property_value", "Acme"},
                     sharingAcmesControllers);
}

TEST_F(RerootControlGraphTest, findsCompaniesSharingAControllerAcrossTwoPatternsSharingThePerson) {
    expectWalkedFrom("MATCH (p:Person)-[:CONTROLS]->(c:Company), (p)-[:CONTROLS]->(acme:Company {name: 'Acme'}) "
                     "WHERE c <> acme RETURN c.name, p.name",
                     {"db.scan_nodes_by_property_value", "Acme"},
                     sharingAcmesControllers);
}

TEST_F(RerootControlGraphTest, findsCompaniesSharingAControllerAcrossTwoPatternsEqualOnThePerson) {
    expectWalkedFrom("MATCH (p:Person)-[:CONTROLS]->(c:Company), (q:Person)-[:CONTROLS]->(acme:Company) "
                     "WHERE p = q AND acme.name = 'Acme' AND c <> acme RETURN c.name, p.name",
                     {"db.scan_nodes_by_property_value", "Acme"},
                     sharingAcmesControllers);
    expectWalkedFrom("MATCH (p:Person)-[:CONTROLS]->(c:Company), (q:Person)-[:CONTROLS]->(acme:Company) "
                     "WHERE q = p AND acme.name = 'Acme' AND c <> acme RETURN c.name, p.name",
                     {"db.scan_nodes_by_property_value", "Acme"},
                     sharingAcmesControllers);
    expectWalkedFrom("MATCH (q:Person)-[:CONTROLS]->(acme:Company), (p:Person)-[:CONTROLS]->(c:Company) "
                     "WHERE p = q AND acme.name = 'Acme' AND c <> acme RETURN c.name, p.name",
                     {"db.scan_nodes_by_property_value", "Acme"},
                     sharingAcmesControllers);
}

TEST_F(RerootControlGraphTest, findsCompaniesSharingAControllerWithACompanyMatchedBefore) {
    expectWalkedFrom("MATCH (acme:Company {name: 'Acme'}) WITH acme "
                     "MATCH (c:Company)<-[:CONTROLS]-(p:Person)-[:CONTROLS]->(x:Company) "
                     "WHERE x = acme AND c <> x RETURN c.name, p.name",
                     {"db.scan_nodes_by_property_value", "Acme"},
                     sharingAcmesControllers);
}

TEST_F(RerootControlGraphTest, countsTheControllersEachCompanySharesWithTheNamedOne) {
    expectWalkedFrom("MATCH (c:Company)<-[:CONTROLS]-(p:Person)-[:CONTROLS]->(acme:Company {name: 'Acme'}) "
                     "WHERE c <> acme RETURN c.name, count(p)",
                     {"db.scan_nodes_by_property_value", "Acme"},
                     {{"Globex", "2"}, {"Initech", "1"}, {"Wonka", "1"}});
    expectWalkedFrom("MATCH (c:Company)<-[:CONTROLS]-(p:Person)-[:CONTROLS]->(acme:Company {name: 'Acme'}) "
                     "WHERE c <> acme RETURN DISTINCT c.name",
                     {"db.scan_nodes_by_property_value", "Acme"},
                     {{"Globex"}, {"Initech"}, {"Wonka"}});
}

TEST_F(RerootControlGraphTest, findsThePersonsControllingACompanyWithTheNamedOne) {
    expectWalkedFrom("MATCH (q:Person)-[:CONTROLS]->(c:Company)<-[:CONTROLS]-(alice:Person {name: 'Alice'}) "
                     "WHERE q <> alice RETURN q.name, c.name",
                     {"db.scan_nodes_by_property_value", "Alice"},
                     coControllingWithAlice);
    expectWalkedFrom("MATCH (p:Person)-[:CONTROLS]->(a:Company), (q:Person)-[:CONTROLS]->(b:Company) "
                     "WHERE a = b AND q.name = 'Alice' AND p <> q RETURN p.name, a.name",
                     {"db.scan_nodes_by_property_value", "Alice"},
                     coControllingWithAlice);
}

TEST_F(RerootControlGraphTest, findsThePersonsTwoNamedCompaniesHaveInCommon) {
    expectWalkedFrom("MATCH (p:Person)-[:CONTROLS]->(a:Company {name: 'Acme'}), (p)-[:CONTROLS]->(g:Company {name: 'Globex'}) "
                     "RETURN p.name",
                     {"db.scan_nodes_by_property_value", "Company"},
                     {{"Alice"}, {"Frank"}});
}

TEST_F(RerootControlGraphTest, keepsTheNamedPersonItStartsFrom) {
    expectWalkedFrom("MATCH (alice:Person {name: 'Alice'})-[:CONTROLS]->(c:Company)<-[:CONTROLS]-(frank:Person {name: 'Frank'}) "
                     "RETURN c.name",
                     {"db.scan_nodes_by_property_value", "Alice"},
                     {{"Acme"}, {"Globex"}});
}

TEST_F(RerootControlGraphTest, walksControlBackFromTheNamedSubsidiary) {
    expectWalkedFrom("MATCH (p:Person)-[:CONTROLS]->(h:Company)-[:CONTROLS]->(s:Company {name: 'Stark'}) "
                     "RETURN p.name, h.name",
                     {"db.scan_nodes_by_property_value", "Stark"},
                     {{"Alice", "Globex"}, {"Carol", "Globex"}, {"Frank", "Globex"}});
}

TEST_F(RerootControlGraphTest, walksThreeHopsBackFromTheNamedCountry) {
    expectWalkedFrom("MATCH (c:Company)<-[:CONTROLS]-(p:Person)-[:CONTROLS]->(o:Company)-[:REGISTERED_IN]->(k:Country {name: 'Panama'}) "
                     "WHERE c <> o RETURN c.name, p.name, o.name",
                     {"db.scan_nodes_by_property_value", "Panama"},
                     {
                         {"Acme", "Alice", "Globex"},
                         {"Acme", "Frank", "Globex"},
                         {"Globex", "Carol", "Hooli"},
                         {"Hooli", "Carol", "Globex"},
                         {"Wayne", "Erin", "Stark"},
                         {"Wonka", "Frank", "Globex"},
                     });
}

// No op reads the edges of both directions by type, so the type checks stay
TEST_F(RerootControlGraphTest, walksUndirectedHopsFromTheNamedCompany) {
    constexpr std::string_view QUERY = "MATCH (c:Company)-[:CONTROLS]-(p:Person)-[:CONTROLS]-(acme:Company {name: 'Acme'}) "
                                       "WHERE c <> acme RETURN c.name, p.name";

    std::string program;
    dbProgramOf(QUERY, program);

    const std::string_view source = firstSourceOf(program);
    EXPECT_TRUE(contains(source, "db.scan_nodes_by_property_value")) << program;
    EXPECT_TRUE(contains(source, "Acme")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;

    expectRows(QUERY, sharingAcmesControllers);
}

// No scan reads a property against a list, so the walk starts from every Company and
// keeps the two named ones before its first hop
TEST_F(RerootControlGraphTest, walksFromTheCompaniesAnInListNames) {
    constexpr std::string_view QUERY = "MATCH (c:Company)<-[:CONTROLS]-(p:Person)-[:CONTROLS]->(acme:Company) "
                                       "WHERE acme.name IN ['Acme', 'Hooli'] AND c <> acme RETURN c.name, p.name, acme.name";

    std::string program;
    dbProgramOf(QUERY, program);

    EXPECT_TRUE(contains(firstSourceOf(program), "db.scan_nodes_by_label([\"Company\"])")) << program;
    EXPECT_LT(program.find("db.in "), program.find("db.get_in_edges")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;

    expectRows(QUERY,
               {
                   {"Globex", "Alice", "Acme"},
                   {"Globex", "Carol", "Hooli"},
                   {"Globex", "Frank", "Acme"},
                   {"Initech", "Bob", "Acme"},
                   {"Wonka", "Frank", "Acme"},
               });
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
