#include <gtest/gtest.h>

#include "LocalMemory.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"
#include "SystemManager.h"
#include "QueryInterpreter.h"
#include "TuringDB.h"
#include "SimpleGraph.h"
#include "QueryTester.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class QueryTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        Graph* graph = _env->getSystemManager().createGraph("simple");
        SimpleGraph::createSimpleGraph(graph);

        _interp = std::make_unique<QueryInterpreter>(&_env->getSystemManager(),
                                                     &_env->getJobSystem());
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreter> _interp {nullptr};
};

TEST_F(QueryTest, ReturnNode) {
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("MATCH (n) RETURN n")
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .execute();

    tester.query("MATCH (n)-[e]-(m) RETURN e")
        .expectVector<EdgeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .execute();

    tester.query("MATCH (n)-[e]-(m) RETURN n")
        .expectVector<NodeID>({0, 0, 0, 0, 1, 1, 1, 6, 8, 8, 9, 9, 11})
        .execute();

    tester.query("MATCH (n)-[e]-(m) RETURN m")
        .expectVector<NodeID>({1, 6, 2, 3, 0, 4, 5, 0, 4, 7, 10, 2, 5})
        .execute();

    tester.query("MATCH (n)-[e]-(m) RETURN n, m")
        .expectVector<NodeID>({0, 0, 0, 0, 1, 1, 1, 6, 8, 8, 9, 9, 11})
        .expectVector<NodeID>({1, 6, 2, 3, 0, 4, 5, 0, 4, 7, 10, 2, 5})
        .execute();

    tester.query("MATCH (n)-[e]-(m) RETURN n, e, m")
        .expectVector<NodeID>({0, 0, 0, 0, 1, 1, 1, 6, 8, 8, 9, 9, 11})
        .expectVector<EdgeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .expectVector<NodeID>({1, 6, 2, 3, 0, 4, 5, 0, 4, 7, 10, 2, 5})
        .execute();

    tester.query("MATCH (n)--(m)--(q)--(r) RETURN n, m, q, r")
        .expectVector<NodeID>({0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 6, 6, 6, 6})
        .expectVector<NodeID>({1, 1, 1, 1, 6, 6, 6, 6, 0, 0, 0, 0, 0, 0, 0, 0})
        .expectVector<NodeID>({0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 6, 1, 1, 1, 6})
        .expectVector<NodeID>({1, 6, 2, 3, 1, 6, 2, 3, 0, 4, 5, 0, 0, 4, 5, 0})
        .execute();
}

TEST_F(QueryTest, EdgeMatching) {
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("MATCH (n)-[e]-(m) RETURN n, e, m")
        .expectVector<NodeID>({0, 0, 0, 0, 1, 1, 1, 6, 8, 8, 9, 9, 11})
        .expectVector<EdgeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .expectVector<NodeID>({1, 6, 2, 3, 0, 4, 5, 0, 4, 7, 10, 2, 5})
        .execute();

    tester.query("MATCH (n)-[e]-(m)-[e1]-(q)-[e2]-(r) RETURN n, e, m, e1, q, e2, r")
        .expectVector<NodeID>({0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 6, 6, 6, 6})
        .expectVector<EdgeID>({0, 0, 0, 0, 1, 1, 1, 1, 4, 4, 4, 4, 7, 7, 7, 7})
        .expectVector<NodeID>({1, 1, 1, 1, 6, 6, 6, 6, 0, 0, 0, 0, 0, 0, 0, 0})
        .expectVector<EdgeID>({4, 4, 4, 4, 7, 7, 7, 7, 0, 0, 0, 1, 0, 0, 0, 1})
        .expectVector<NodeID>({0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 6, 1, 1, 1, 6})
        .expectVector<EdgeID>({0, 1, 2, 3, 0, 1, 2, 3, 4, 5, 6, 7, 4, 5, 6, 7})
        .expectVector<NodeID>({1, 6, 2, 3, 1, 6, 2, 3, 0, 4, 5, 0, 0, 4, 5, 0})
        .execute();


    tester.query("MATCH (n:Person)-[e]-(m:Interest) RETURN n, n.name, e.name, m.name, m")
        .expectVector<NodeID>({0, 0, 0, 1, 1, 8, 8, 9, 9, 11})
        .expectOptVector<types::String::Primitive>({
            "Remy",
            "Remy",
            "Remy",
            "Adam",
            "Adam",
            "Maxime",
            "Maxime",
            "Luc",
            "Luc",
            "Martina",
        })
        .expectOptVector<types::String::Primitive>({
            "Remy -> Ghosts",
            "Remy -> Computers",
            "Remy -> Eighties",
            "Adam -> Bio",
            "Adam -> Cooking",
            "Maxime -> Bio",
            "Maxime -> Paddle",
            "Luc -> Animals",
            "Luc -> Computers",
            "Martina -> Cooking",
        })
        .expectOptVector<types::String::Primitive>({
            "Ghosts",
            "Computers",
            "Eighties",
            "Bio",
            "Cooking",
            "Bio",
            "Paddle",
            "Animals",
            "Computers",
            "Cooking",
        })
        .expectVector<NodeID>({6, 2, 3, 4, 5, 4, 7, 10, 2, 5})
        .execute();
}

TEST_F(QueryTest, MatchWildcard) {
    QueryTester tester {_env->getMem(), *_interp};
    tester.query("MATCH (n) RETURN *")
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .execute();

    tester.query("MATCH (n)--(m) RETURN *")
        .expectVector<NodeID>({0, 0, 0, 0, 1, 1, 1, 6, 8, 8, 9, 9, 11})
        .expectVector<EdgeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .expectVector<NodeID>({1, 6, 2, 3, 0, 4, 5, 0, 4, 7, 10, 2, 5})
        .execute();

    tester.query("MATCH (n:Person)--(m:Person) RETURN n.name, *")
        .expectOptVector<types::String::Primitive>({"Remy", "Adam"})
        .expectVector<NodeID>({0, 1})
        .expectVector<EdgeID>({0, 4})
        .expectVector<NodeID>({1, 0})
        .execute();
}

TEST_F(QueryTest, MatchNodeLabel) {
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("MATCH (n:Person) RETURN n")
        .expectVector<NodeID>({0, 1, 8, 9, 11, 12})
        .execute();

    tester.query("MATCH (n:Interest) RETURN n")
        .expectVector<NodeID>({2, 3, 4, 5, 6, 7, 10})
        .execute();

    tester.query("MATCH (n:Founder) RETURN n, n.name")
        .expectVector<NodeID>({0, 1})
        .expectOptVector<types::String::Primitive>({"Remy", "Adam"})
        .execute();

    tester.query("MATCH (n:Founder,SoftwareEngineering) RETURN n")
        .expectVector<NodeID>({0})
        .execute();

    tester.query("MATCH (n:SoftwareEngineering) RETURN n")
        .expectVector<NodeID>({0, 2, 9, 12})
        .execute();
}

TEST_F(QueryTest, MatchEdgeType) {
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("MATCH (n)-[e:INTERESTED_IN]-(m) RETURN n, e, m")
        .expectVector<NodeID>({0, 0, 0, 1, 1, 8, 8, 9, 9, 11})
        .expectVector<EdgeID>({1, 2, 3, 5, 6, 8, 9, 10, 11, 12})
        .expectVector<NodeID>({6, 2, 3, 4, 5, 4, 7, 10, 2, 5})
        .execute();

    tester.query("MATCH (n)-[e:KNOWS_WELL]-(m) RETURN n, n.name, e, e.name, m, m.name")
        .expectVector<NodeID>({0, 1, 6})
        .expectOptVector<types::String::Primitive>({
            "Remy",
            "Adam",
            "Ghosts",
        })
        .expectVector<EdgeID>({0, 4, 7})
        .expectOptVector<types::String::Primitive>({
            "Remy -> Adam",
            "Adam -> Remy",
            "Ghosts -> Remy",
        })
        .expectVector<NodeID>({1, 0, 0})
        .expectOptVector<types::String::Primitive>({
            "Adam",
            "Remy",
            "Remy",
        })
        .execute();

    tester.query("MATCH (n)-[e:DOES_NOT_EXIST]-(m) RETURN n, e, m")
        .expectVector<NodeID>({})
        .expectVector<EdgeID>({})
        .expectVector<NodeID>({})
        .execute();
}

TEST_F(QueryTest, NodePropertyProjection) {
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("MATCH (n) RETURN n, n.name")
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .expectOptVector<types::String::Primitive>({
            "Remy",
            "Adam",
            "Computers",
            "Eighties",
            "Bio",
            "Cooking",
            "Ghosts",
            "Paddle",
            "Maxime",
            "Luc",
            "Animals",
            "Martina",
            "Suhas",
        })
        .execute();

    tester.query("MATCH (n:Person) RETURN n, n.name")
        .expectVector<NodeID>({0, 1, 8, 9, 11, 12})
        .expectOptVector<types::String::Primitive>({"Remy", "Adam", "Maxime", "Luc", "Martina", "Suhas"})
        .execute();

    tester.query("MATCH (n:Interest) RETURN n, n.name")
        .expectVector<NodeID>({2, 3, 4, 5, 6, 7, 10})
        .expectOptVector<types::String::Primitive>({"Computers", "Eighties", "Bio", "Cooking", "Ghosts", "Paddle", "Animals"})
        .execute();

    tester.query("MATCH (n:Founder) RETURN n, n.name")
        .expectVector<NodeID>({0, 1})
        .expectOptVector<types::String::Primitive>({"Remy", "Adam"})
        .execute();

    tester.query("MATCH (n:Founder,SoftwareEngineering) RETURN n, n.name")
        .expectVector<NodeID>({0})
        .expectOptVector<types::String::Primitive>({"Remy"})
        .execute();

    tester.query("MATCH (n:SoftwareEngineering) RETURN n, n.name")
        .expectVector<NodeID>({0, 2, 9, 12})
        .expectOptVector<types::String::Primitive>({"Remy", "Computers", "Luc", "Suhas"})
        .execute();

    tester.query("MATCH (n) RETURN n.doesnotexist")
        .expectError()
        .execute();
}

TEST_F(QueryTest, EdgePropertyProjection) {
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("MATCH (n)-[e]-(m) RETURN e.name")
        .expectOptVector<types::String::Primitive>({
            "Remy -> Adam",
            "Remy -> Ghosts",
            "Remy -> Computers",
            "Remy -> Eighties",
            "Adam -> Remy",
            "Adam -> Bio",
            "Adam -> Cooking",
            "Ghosts -> Remy",
            "Maxime -> Bio",
            "Maxime -> Paddle",
            "Luc -> Animals",
            "Luc -> Computers",
            "Martina -> Cooking",
        })
        .execute();

    tester.query("MATCH (n:Person)--(m:Interest) RETURN n, n.name, m, m.name")
        .expectVector<NodeID>({0, 0, 0, 1, 1, 8, 8, 9, 9, 11})
        .expectOptVector<types::String::Primitive>({
            "Remy",
            "Remy",
            "Remy",
            "Adam",
            "Adam",
            "Maxime",
            "Maxime",
            "Luc",
            "Luc",
            "Martina",
        })
        .expectVector<NodeID>({6, 2, 3, 4, 5, 4, 7, 10, 2, 5})
        .expectOptVector<types::String::Primitive>({
            "Ghosts",
            "Computers",
            "Eighties",
            "Bio",
            "Cooking",
            "Bio",
            "Paddle",
            "Animals",
            "Computers",
            "Cooking",
        })
        .execute();

    tester.query("MATCH (n)-[e]-(m) RETURN e.doesnotexist")
        .expectError();

    tester.query("MATCH (n)-[e:INTERESTED_IN]-(m) RETURN n, n.name, e, e.name, m, m.name")
        .expectVector<NodeID>({0, 0, 0, 1, 1, 8, 8, 9, 9, 11})
        .expectOptVector<types::String::Primitive>({
            "Remy",
            "Remy",
            "Remy",
            "Adam",
            "Adam",
            "Maxime",
            "Maxime",
            "Luc",
            "Luc",
            "Martina",
        })
        .expectVector<EdgeID>({1, 2, 3, 5, 6, 8, 9, 10, 11, 12})
        .expectOptVector<types::String::Primitive>({
            "Remy -> Ghosts",
            "Remy -> Computers",
            "Remy -> Eighties",
            "Adam -> Bio",
            "Adam -> Cooking",
            "Maxime -> Bio",
            "Maxime -> Paddle",
            "Luc -> Animals",
            "Luc -> Computers",
            "Martina -> Cooking",
        })
        .expectVector<NodeID>({6, 2, 3, 4, 5, 4, 7, 10, 2, 5})
        .expectOptVector<types::String::Primitive>({
            "Ghosts",
            "Computers",
            "Eighties",
            "Bio",
            "Cooking",
            "Bio",
            "Paddle",
            "Animals",
            "Computers",
            "Cooking",
        })
        .execute();

    tester.query("MATCH (n)-[e]-(m) RETURN e.doesnotexist")
        .expectError();
}

TEST_F(QueryTest, PropertyConstraints) {
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("MATCH (n{hasPhD:true}) RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Remy", "Adam", "Luc", "Martina"})
        .execute();

    tester.query("MATCH (n{hasPhD:true, isFrench: false}) RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Martina"})
        .execute();

    tester.query("MATCH (n:SoftwareEngineering{hasPhD:false}) RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Suhas"})
        .execute();

    tester.query("MATCH (n:Person,SoftwareEngineering{hasPhD:false, isFrench: false}) RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Suhas"})
        .execute();

    tester.query("MATCH (n)-[e{proficiency:\"expert\"}]-(m) RETURN n.name,m.name")
        .expectOptVector<types::String::Primitive>({"Remy", "Remy", "Ghosts", "Maxime"})
        .expectOptVector<types::String::Primitive>({"Ghosts", "Computers", "Remy", "Paddle"})
        .execute();

    tester.query("MATCH (n)-[e{proficiency:\"expert\", duration:20}]-(m) RETURN n.name,m.name")
        .expectOptVector<types::String::Primitive>({"Remy"})
        .expectOptVector<types::String::Primitive>({"Ghosts"})
        .execute();

    tester.query("MATCH (n)-[e{proficiency:\"expert\"}]-(m{isReal:true}) RETURN n.name,m.name")
        .expectOptVector<types::String::Primitive>({"Remy", "Remy"})
        .expectOptVector<types::String::Primitive>({"Ghosts", "Computers"})
        .execute();

    tester.query("MATCH (n)-[e{proficiency:\"expert\"}]-(m:SoftwareEngineering{isReal:true}) RETURN n.name,m.name")
        .expectOptVector<types::String::Primitive>({"Remy"})
        .expectOptVector<types::String::Primitive>({"Computers"})
        .execute();

    tester.query("MATCH (n)-[e:KNOWS_WELL{duration:20}]-(m:SoftwareEngineering) RETURN n.name,m.name")
        .expectOptVector<types::String::Primitive>({"Adam"})
        .expectOptVector<types::String::Primitive>({"Remy"})
        .execute();

    tester.query("MATCH (n)-[e:INTERESTED_IN{duration:20}]-(m:Exotic{isReal:true}) RETURN n.name,m.name")
        .expectOptVector<types::String::Primitive>({"Remy"})
        .expectOptVector<types::String::Primitive>({"Ghosts"})
        .execute();
}

TEST_F(QueryTest, ChangeQuery) {
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("CHANGE NEW")
        .expectVector<const Change*>({}, false)
        .execute();

    const auto changes = tester.query("CHANGE LIST")
                             .expectVector<const Change*>({}, false)
                             .execute()
                             .outputColumnVector<const Change*>(0);

    ASSERT_TRUE(changes);

    tester.setChangeID(changes.value()->back()->id());

    tester.query(
              R"(CREATE (n:Person { name: "New person" })
                          -[:NEW_EDGE_TYPE { name: "New edge" } ]
                          -(m:Interest { name: "New interest" }))")
        .execute();

    tester.query(
              R"(CREATE (n:Interest { name: "Interest 1" }),
                        (m:Interest { name: "Interest 2" }),
                        (p:Interest { name: "Interest 3" }))")
        .execute();

    tester.query("CREATE (n @ 9)-[e:INTERESTED_IN { name: \"Luc -> Video games\" }]-(m:Interest { name: \"Video games\" })")
        .execute();

    tester.query("COMMIT").execute();

    tester.query("MATCH (n:Person) RETURN n.name")
        .expectOptVector<types::String::Primitive>({
            "Remy",
            "Adam",
            "Maxime",
            "Luc",
            "Martina",
            "Suhas",
            "New person",
        })
        .execute();

    tester.query("MATCH (n:Interest) RETURN n.name")
        .expectOptVector<types::String::Primitive>({
            "Computers",
            "Eighties",
            "Bio",
            "Cooking",
            "Ghosts",
            "Paddle",
            "Animals",
            "New interest",
            "Interest 1",
            "Interest 2",
            "Interest 3",
            "Video games",
        })
        .execute();

    tester.query("MATCH (n)-[e]-(m) RETURN e.name")
        .expectOptVector<types::String::Primitive>({
            "Remy -> Adam",
            "Remy -> Ghosts",
            "Remy -> Computers",
            "Remy -> Eighties",
            "Adam -> Remy",
            "Adam -> Bio",
            "Adam -> Cooking",
            "Ghosts -> Remy",
            "Maxime -> Bio",
            "Maxime -> Paddle",
            "Luc -> Animals",
            "Luc -> Computers",
            "Martina -> Cooking",
            "Luc -> Video games",
            "New edge",
        })
        .execute();

    tester.query("MATCH (n)-[e:NEW_EDGE_TYPE]-(m) RETURN e.name")
        .expectOptVector<types::String::Primitive>({"New edge"})
        .execute();

    tester.query("CHANGE SUBMIT")
        .execute();

    tester.setChangeID(ChangeID::head());
    tester.setCommitHash(CommitHash::head());

    tester.query("MATCH (n:Person) RETURN n.name")
        .expectOptVector<types::String::Primitive>({
            "Remy",
            "Adam",
            "Maxime",
            "Luc",
            "Martina",
            "Suhas",
            "New person",
        })
        .execute();
}

TEST_F(QueryTest, ChangeWithRebaseQueries) {
    QueryTester tester {_env->getMem(), *_interp};

    auto change1Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);

    ASSERT_TRUE(change1Res);

    const ChangeID change1 = change1Res.value()->back()->id();

    fmt::print("ChangeID 1: {}\n", change1.get());
    tester.setChangeID(change1);

    tester.query("CREATE (n:TestNode1 { name: \"1\" })-[e:TestEdge1 { name: \"1->2\" }]-(m:TestNode1 { name: \"2\" })")
        .execute();

    auto change2Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);

    const ChangeID change2 = change2Res.value()->back()->id();

    fmt::print("ChangeID 2: {}\n", change2.get());
    tester.setChangeID(change2);

    tester.query("CREATE (n:TestNode2 { name: \"3\" })-[e:TestEdge2 { name: \"3->4\" }]-(m:TestNode2 { name: \"4\" })")
        .execute();

    tester.query("CHANGE SUBMIT")
        .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("MATCH (n:TestNode2)-[e:TestEdge2]-(m:TestNode2) RETURN n.name, e.name, m.name")
        .expectOptVector<types::String::Primitive>({"3"})
        .expectOptVector<types::String::Primitive>({"3->4"})
        .expectOptVector<types::String::Primitive>({"4"})
        .execute();

    tester.setChangeID(change1);

    tester.query("CHANGE SUBMIT")
        .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("MATCH (n:TestNode1)-[e:TestEdge1]-(m:TestNode1) RETURN n.name, e.name, m.name")
        .expectOptVector<types::String::Primitive>({"1"})
        .expectOptVector<types::String::Primitive>({"1->2"})
        .expectOptVector<types::String::Primitive>({"2"})
        .execute();

    tester.query("MATCH (n:TestNode2)-[e:TestEdge2]-(m:TestNode2) RETURN n.name, e.name, m.name")
        .expectOptVector<types::String::Primitive>({"3"})
        .expectOptVector<types::String::Primitive>({"3->4"})
        .expectOptVector<types::String::Primitive>({"4"})
        .execute();
}

TEST_F(QueryTest, ChangeWithRebaseFromEmpty) {
    QueryTester tester {_env->getMem(), *_interp, "default"};

    auto change1Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);

    ASSERT_TRUE(change1Res);

    const ChangeID change1 = change1Res.value()->back()->id();

    auto change2Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);

    const ChangeID change2 = change2Res.value()->back()->id();

    // First change
    tester.setChangeID(change1);
    tester.query(R"(create (n:Person {"name": "Luc"}))")
        .execute();

    tester.query("CHANGE SUBMIT")
        .execute();

    tester.setChangeID(change2);

    tester.query(R"(create (n:Person {"name": "Remy"}))")
        .execute();

    tester.query("CHANGE SUBMIT")
        .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("MATCH (n:Person) RETURN n, n.name")
        .expectVector<NodeID>({0, 1})
        .expectOptVector<types::String::Primitive>({"Luc", "Remy"})
        .execute();
}

TEST_F(QueryTest, threeCreateTargets) {
    QueryTester tester {_mem, *_interp, "default"};

    const auto newChange = [&]() {
        auto res = tester.query("CHANGE NEW")
                       .expectVector<const Change*>({}, false)
                       .execute()
                       .outputColumnVector<const Change*>(0);
        const ChangeID id = res.value()->back()->id();
        tester.setChangeID(id);
        return id;
    };

    const ChangeID change1 = newChange();

    tester.setChangeID(change1);

    tester.query("create (n:FSTNODE), (m:SNDNODE), (n)-[e:NEWEDGE]-(m)")
        .execute();

    tester.query("change submit")
        .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("match (n) return n")
        .expectVector<NodeID>({0,1})
        .execute();

    tester.query("match (n)-[e]-(m) return n,e,m")
        .expectVector<NodeID>({0})
        .expectVector<EdgeID>({0})
        .expectVector<NodeID>({1})
        .execute();
}

TEST_F(QueryTest, manyCreateTargets) {
    QueryTester tester {_mem, *_interp, "default"};

    const auto newChange = [&]() {
        auto res = tester.query("CHANGE NEW")
                       .expectVector<const Change*>({}, false)
                       .execute()
                       .outputColumnVector<const Change*>(0);
        const ChangeID id = res.value()->back()->id();
        tester.setChangeID(id);
        return id;
    };

    const ChangeID change1 = newChange();

    tester.setChangeID(change1);

    tester.query("create (n:FSTNODE), (m:SNDNODE), (s:NEWSRC)-[f:NEWEDGE]-(t:NEWTGT), (o:IRRELEVANTNODE), (p:IRRELEVEANTNODE), (n)-[e:NEWEDGE]-(m)")
        .execute();

    tester.query("change submit")
        .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("match (n) return n")
        .expectVector<NodeID>({0,1,2,3,4,5})
        .execute();

    tester.query("match (n)-[e]-(m) return n,e,m")
        .expectVector<NodeID>({0, 2})
        .expectVector<EdgeID>({0, 1})
        .expectVector<NodeID>({1, 3})
        .execute();
}

TEST_F(QueryTest, edgeSpanningCommits) {
    QueryTester tester {_env->getMem(), *_interp, "default"};

    auto change1Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);
    ASSERT_TRUE(change1Res);
    const ChangeID change1 = change1Res.value()->back()->id();

    tester.setChangeID(change1);

    tester.query("create (n:SOURCENODE)")
        .execute();

    tester.query("commit")
        .execute();

    tester.query("match (n) return n")
        .expectVector<NodeID>({0})
        .execute();
    tester.query("call labels ()")
        .expectVector<LabelID>({0})
        .expectVector<std::string_view>({"SOURCENODE"})
        .execute();

    tester.query("create (n @ 0)-[e:FORWARDSEDGE]-(m:TARGETNODE)")
        .execute();

    tester.query("create (m:TARGETNODE)-[e:FORWARDSEDGE]-(n @ 0)")
        .execute();

    tester.query("commit")
        .execute();

    tester.query("match (n) return n")
        .expectVector<NodeID>({0, 1, 2})
        .execute();

    tester.query("match (n)-[e]-(m) return n,e,m")
        .expectVector<NodeID>({0, 2})
        .expectVector<EdgeID>({0, 1})
        .expectVector<NodeID>({1, 0})
        .execute();

    tester.query("call labels ()")
        .expectVector<LabelID>({0, 1})
        .expectVector<std::string_view>({"SOURCENODE", "TARGETNODE"})
        .execute();

    tester.setChangeID(ChangeID::head());

    auto change2Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);
    ASSERT_TRUE(change2Res);
    const ChangeID change2 = change2Res.value()->back()->id();

    tester.setChangeID(change2);

    // Do exactly the same thing
    tester.query("create (n:SOURCENODE)")
        .execute();

    tester.query("commit")
        .execute();

    tester.query("match (n) return n")
        .expectVector<NodeID>({0})
        .execute();
    tester.query("call labels ()")
        .expectVector<LabelID>({0})
        .expectVector<std::string_view>({"SOURCENODE"})
        .execute();

    tester.query("create (n @ 0)-[e:FORWARDSEDGE]-(m:TARGETNODE)")
        .execute();

    tester.query("create (m:TARGETNODE)-[e:FORWARDSEDGE]-(n @ 0)")
        .execute();

    tester.query("commit")
        .execute();

    tester.query("match (n) return n")
        .expectVector<NodeID>({0, 1, 2})
        .execute();

    tester.query("match (n)-[e]-(m) return n,e,m")
        .expectVector<NodeID>({0, 2})
        .expectVector<EdgeID>({0, 1})
        .expectVector<NodeID>({1, 0})
        .execute();

    tester.query("call labels ()")
        .expectVector<LabelID>({0, 1})
        .expectVector<std::string_view>({"SOURCENODE", "TARGETNODE"})
        .execute();

    tester.query("change submit")
        .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("match (n)-[e]-(m) return n,e,m")
        .expectVector<NodeID>({0, 2})
        .expectVector<EdgeID>({0, 1})
        .expectVector<NodeID>({1, 0})
        .execute();

    tester.query("call labels ()")
        .expectVector<LabelID>({0, 1})
        .expectVector<std::string_view>({"SOURCENODE", "TARGETNODE"})
        .execute();

    tester.setChangeID(change1);
    tester.query("CHANGE SUBMIT")
        .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("match (n) return n")
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5})
        .execute();

    tester.query("match (n)-[e]-(m) return n,e,m")
        .expectVector<NodeID>({0, 2, 3, 5})
        .expectVector<EdgeID>({0, 1, 2, 3})
        .expectVector<NodeID>({1, 0, 4, 3})
        .execute();

    tester.query("call labels ()")
        .expectVector<LabelID>({0, 1})
        .expectVector<std::string_view>({"SOURCENODE", "TARGETNODE"})
        .execute();
}

TEST_F(QueryTest, threeChangeRebase) {
    QueryTester tester {_env->getMem(), *_interp, "default"};

    const auto newChange = [&]() {
        auto res = tester.query("CHANGE NEW")
                       .expectVector<const Change*>({}, false)
                       .execute()
                       .outputColumnVector<const Change*>(0);
        const ChangeID id = res.value()->back()->id();
        tester.setChangeID(id);
        return id;
    };

    // Make local changes
    auto makeChanges = [&]() {
        ChangeID changeID = newChange();
        tester.setChangeID(changeID);
        { // Change
            tester.query("create (n:NODE)")
                .execute();
            tester.query("commit")
                .execute();
            tester.query("match (n) return n")
                .expectVector<NodeID>({0})
                .execute();
            tester.query("create (n @ 0)-[:EDGE]-(m:NODE)")
                .execute();
            tester.query("commit")
                .execute();
            tester.query("match (n)-[e]-(m) return n,e,m")
                .expectVector<NodeID>({0})
                .expectVector<EdgeID>({0})
                .expectVector<NodeID>({1})
                .execute();
        }
        return changeID;
    };

    // Make local changes on three independent changes
    ChangeID change1 = makeChanges();
    ChangeID change2 = makeChanges();
    ChangeID change3 = makeChanges();

    // Submit the newest change
    tester.setChangeID(change3);
    tester.query("change submit")
        .execute();

    { // Verify on main. Normal submit
        tester.setChangeID(ChangeID::head());
        tester.query("match (n)-[e]-(m) return n,e,m")
            .expectVector<NodeID>({0})
            .expectVector<EdgeID>({0})
            .expectVector<NodeID>({1})
            .execute();
    }

    // Submit the second newest change
    tester.setChangeID(change2);
    tester.query("change submit")
        .execute();

    { // Verify on main. Ensure correct rebase
        tester.setChangeID(ChangeID::head());
        tester.query("match (n)-[e]-(m) return n,e,m")
            .expectVector<NodeID>({0, 2}) // ID 2 on main = ID 0 on Change2
            .expectVector<EdgeID>({0, 1}) // ID 1 on main = ID 0 on Change2
            .expectVector<NodeID>({1, 3}) // ID 3 on main = ID 1 on Change2
            .execute();
    }

    // Submit the oldest change
    tester.setChangeID(change1);
    tester.query("change submit")
        .execute();

    { // Verify on main. Ensure correct rebase
        tester.setChangeID(ChangeID::head());
        tester.query("match (n)-[e]-(m) return n,e,m")
            .expectVector<NodeID>({0, 2, 4}) // ID 4 on main = ID 0 on Change2
            .expectVector<EdgeID>({0, 1, 2}) // ID 2 on main = ID 0 on Change2
            .expectVector<NodeID>({1, 3, 5}) // ID 5 on main = ID 1 on Change2
            .execute();
    }
}

TEST_F(QueryTest, threeChangeRebaseLabelsProps) {
    QueryTester tester {_mem, *_interp, "default"};

    constexpr size_t CHANGE1_SIZE = 3;
    constexpr size_t CHANGE2_SIZE = 2;
    constexpr size_t CHANGE3_SIZE = 5;

    auto change1Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);

    ASSERT_TRUE(change1Res);

    const ChangeID change1 = change1Res.value()->back()->id();

    auto change2Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);

    const ChangeID change2 = change2Res.value()->back()->id();

    tester.setChangeID(change1);
    for (size_t i = 0; i < CHANGE1_SIZE; i++) {
        tester.query(R"(create (n:CHANGE1LABEL {"changeid": "ONE"}))")
            .execute();
    }

    tester.setChangeID(change2);

    for (size_t i = 0; i < CHANGE2_SIZE; i++) {
        tester.query(R"(create (n:CHANGE2LABEL {"changeid": "TWO"}))")
            .execute();
    }

    tester.query("CHANGE SUBMIT")
        .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("MATCH (n) RETURN n, n.changeid")
        .expectVector<NodeID>({0,1})
        .expectOptVector<types::String::Primitive>({"TWO", "TWO"})
        .execute();

    tester.query("CALL LABELS()")
        .expectVector<LabelID>({0})
        .expectVector<std::string_view>({"CHANGE2LABEL"})
        .execute();

    auto change3Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);
    const ChangeID change3 = change3Res.value()->back()->id();

    tester.setChangeID(change3);
    for (size_t i = 0; i < CHANGE3_SIZE; i++) {
        tester.query(R"(create (n:CHANGE3LABEL {"changeid": "THREE"}))")
            .execute();
    }
    tester.query("CHANGE SUBMIT")
        .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("MATCH (n) RETURN n, n.changeid")
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 6})
        .expectOptVector<types::String::Primitive>(
            {"TWO", "TWO", "THREE", "THREE", "THREE", "THREE", "THREE"})
        .execute();

    tester.query("CALL LABELS()")
        .expectVector<LabelID>({0,1})
        .expectVector<std::string_view>({"CHANGE2LABEL", "CHANGE3LABEL"})
        .execute();

    tester.setChangeID(change1);
    tester.query("CHANGE SUBMIT")
        .execute();

    tester.setChangeID(ChangeID::head());

    tester.query("MATCH (n) RETURN n, n.changeid")
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        .expectOptVector<types::String::Primitive>({"TWO", "TWO", "THREE", "THREE",
                                                    "THREE", "THREE", "THREE", "ONE",
                                                    "ONE", "ONE"})
        .execute();

    tester.query("CALL LABELS()")
        .expectVector<LabelID>({0, 1, 2})
        .expectVector<std::string_view>({"CHANGE2LABEL", "CHANGE3LABEL", "CHANGE1LABEL"})
        .execute();
}

/*
 * Multiple concurrent changes, committed locally, then submitted in a different order,
 * rebasing all of : NodeIDs, EdgeIDs, PropertyIDs, LabelIDs, LabelSets
 */
TEST_F(QueryTest, threeChangeRebaseDifferingProps) {
    QueryTester tester {_mem, *_interp, "default"};

    constexpr size_t CHANGE0_SIZE = 4;
    constexpr size_t CHANGE1_SIZE = 1;
    constexpr size_t CHANGE2_SIZE = 2;
    constexpr std::array<size_t, 3> SIZES = {CHANGE0_SIZE, CHANGE1_SIZE, CHANGE2_SIZE};

    const auto newChange = [&](){
        auto res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);
        const ChangeID id = res.value()->back()->id();
        tester.setChangeID(id);
        return id;
    };

    auto makeNodePtn = [](ChangeID id) -> std::string {
        return fmt::format(":NODE_ID_{0}{{property_{0}:\"value_{0}\"}}",
                           id.get());
    };

    auto makeEdgePtn = [](ChangeID id) -> std::string {
        return fmt::format(":EDGE_ID_{0}{{edge_property_{0}:\"value_{0}\"}}",
                           id.get());
    };

    // Create various changes, all with unique properties and labels
    std::vector<ChangeID> changeIDs;
    for (size_t size : SIZES) {
        const ChangeID thisChangeID = newChange();
        changeIDs.emplace_back(thisChangeID);

        std::string nodePtn = makeNodePtn(thisChangeID);
        std::string edgePtn = makeEdgePtn(thisChangeID);

        for (size_t i = 0; i < size; i++) {
            std::string query =
                fmt::format("create (n{})-[e{}]-(m{})", nodePtn, edgePtn, nodePtn);
            tester.query(query)
                  .execute();
        }
        tester.query("commit");
    }

    // Submit the changes in reverse order
    auto idIt = changeIDs.rbegin();
    // auto szIt = SIZES.rbegin();

    { // Change2: Creates 2 edges, 4 nodes
        ChangeID id = *idIt;
        tester.setChangeID(id);
        tester.query("change submit")
               .execute();
        tester.setChangeID(ChangeID::head());

        std::string nodePtn = makeNodePtn(id);
        std::string edgePtn = makeEdgePtn(id);

        std::string query = fmt::format("match (n{})-[e{}]-(m{}) return n,e,m", nodePtn,
                                        edgePtn, nodePtn);

        tester.query(query)
              .expectVector<NodeID>({0,2})
              .expectVector<EdgeID>({0,1})
              .expectVector<NodeID>({1,3})
              .execute();

        tester.query("call labels ()")
            .expectVector<LabelID>({0})
            .expectVector<std::string_view>({"NODE_ID_2"})
            .execute();

        tester.query("call properties ()")
            .expectVector<PropertyTypeID>({0, 1})
            .expectVector<std::string_view>({"property_2", "edge_property_2"})
            .expectVector<std::string_view>({"String", "String"})
            .execute();
    }

    { // Change1: Creates 1 edge, 2 nodes
        ChangeID id = *(++idIt);
        tester.setChangeID(id);
        tester.query("change submit")
               .execute();
        tester.setChangeID(ChangeID::head());

        std::string nodePtn = makeNodePtn(id);
        std::string edgePtn = makeEdgePtn(id);

        std::string query = fmt::format("match (n{})-[e{}]-(m{}) return n,e,m", nodePtn,
                                        edgePtn, nodePtn);

        tester.query(query)
              .expectVector<NodeID>({4})
              .expectVector<EdgeID>({2})
              .expectVector<NodeID>({5})
              .execute();

        tester.query("call labels ()")
            .expectVector<LabelID>({0, 1})
            .expectVector<std::string_view>({"NODE_ID_2", "NODE_ID_1"})
            .execute();

        tester.query("call properties ()")
            .expectVector<PropertyTypeID>({0, 1, 2, 3})
            .expectVector<std::string_view>(
                {"property_2", "edge_property_2", "property_1", "edge_property_1"})
            .expectVector<std::string_view>({"String", "String", "String", "String"})
            .execute();
    }

    { // Change0: Creates 4 edges, 8 nodes
        ChangeID id = *(++idIt);
        tester.setChangeID(id);
        tester.query("change submit").execute();
        tester.setChangeID(ChangeID::head());

        std::string nodePtn = makeNodePtn(id);
        std::string edgePtn = makeEdgePtn(id);

        std::string query = fmt::format("match (n{})-[e{}]-(m{}) return n,e,m",
                                        nodePtn, edgePtn, nodePtn);

        tester.query(query)
            .expectVector<NodeID>({6, 8, 10, 12})
            .expectVector<EdgeID>({3, 4, 5,  6})
            .expectVector<NodeID>({7, 9, 11, 13})
            .execute();

        tester.query("call labels ()")
            .expectVector<LabelID>({0, 1, 2})
            .expectVector<std::string_view>({"NODE_ID_2", "NODE_ID_1", "NODE_ID_0"})
            .execute();

        tester.query("call properties ()")
            .expectVector<PropertyTypeID>({0, 1, 2, 3, 4, 5})
            .expectVector<std::string_view>({"property_2", "edge_property_2",
                                             "property_1", "edge_property_1",
                                             "property_0", "edge_property_0"})
            .expectVector<std::string_view>(
                {"String", "String", "String", "String", "String", "String"})
            .execute();
    }
}

TEST_F(QueryTest, changeCommitsThenRebase) {
    QueryTester tester {_mem, *_interp, "default"};

    auto change1Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);
    ASSERT_TRUE(change1Res);
    const ChangeID change1 = change1Res.value()->back()->id();

    tester.setChangeID(change1);

    tester.query(R"(create (n:CHANGE1LABEL {"id":1, "changeid": "ONE", "committed":true}))")
        .execute();

    // Commit changes locally
    tester.query("COMMIT")
        .execute();

    tester.query("match (n) return n, n.id, n.changeid, n.committed")
        .expectVector<NodeID>({0})
        .expectOptVector<types::Int64::Primitive>({1})
        .expectOptVector<types::String::Primitive>({"ONE"})
        .expectOptVector<types::Bool::Primitive>({true})
        .execute();
    tester.query("call properties ()")
        .expectVector<PropertyTypeID>({0, 1, 2})
        .expectVector<std::string_view>({"id", "changeid", "committed"})
        .expectVector<std::string_view>({"Int64", "String", "Bool"})
        .execute();

    tester.query(R"(create (n:CHANGE1LABEL {cheeky=true, "id":2, "changeid": "ONE", "committed":true}))")
        .execute();
    tester.query(R"(create (n:CHANGE1LABEL {cheeky=true, "id":3, "changeid": "ONE", "committed":true, cheeky=true}))")
        .execute();

    // Commit changes locally
    tester.query("COMMIT")
        .execute();

    tester.query("match (n) return n, n.id, n.changeid, n.committed, n.cheeky")
        .expectVector<NodeID>({0,1,2})
        .expectOptVector<types::Int64::Primitive>({1,2,3})
        .expectOptVector<types::String::Primitive>({"ONE", "ONE", "ONE"})
        .expectOptVector<types::Bool::Primitive>({true, true, true})
        .expectOptVector<types::Bool::Primitive>({std::nullopt, true, true})
        .execute();
    tester.query("call properties ()")
        .expectVector<PropertyTypeID>({0, 1, 2, 3})
        .expectVector<std::string_view>({"id", "changeid", "committed", "cheeky"})
        .expectVector<std::string_view>({"Int64", "String", "Bool", "Bool"})
        .execute();

    auto change2Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);
    ASSERT_TRUE(change2Res);
    const ChangeID change2 = change2Res.value()->back()->id();

    tester.setChangeID(change2);

    tester.query(R"(create (n:CHANGE1LABEL {"id":4, "changeid": "TWO", "committed":false}))")
        .execute();
    tester.query(R"(create (n:CHANGE1LABEL {"id":5, "changeid": "TWO", "committed":false}))")
        .execute();

    tester.query("CHANGE SUBMIT")
        .execute();

    tester.setChangeID(ChangeID::head());
    tester.query("match (n) return n, n.id, n.changeid, n.committed")
        .expectVector<NodeID>({0, 1})
        .expectOptVector<types::Int64::Primitive>({4, 5})
        .expectOptVector<types::String::Primitive>({"TWO", "TWO"})
        .expectOptVector<types::Bool::Primitive>({false, false})
        .execute();
    tester.query("call properties ()")
        .expectVector<PropertyTypeID>({0, 1, 2})
        .expectVector<std::string_view>({"id", "changeid", "committed"})
        .expectVector<std::string_view>({"Int64", "String", "Bool"})
        .execute();


    tester.setChangeID(change1);

    tester.query("CHANGE SUBMIT")
        .execute();


    tester.setChangeID(ChangeID::head());
    tester.query("match (n) return n, n.id, n.changeid, n.committed")
        .expectVector<NodeID>({0, 1, 2, 3, 4})
        .expectOptVector<types::Int64::Primitive>({4, 5, 1, 2, 3})
        .expectOptVector<types::String::Primitive>({"TWO", "TWO", "ONE", "ONE", "ONE"})
        .expectOptVector<types::Bool::Primitive>({false, false, true, true, true})
        .execute();
    tester.query("call properties ()")
        .expectVector<PropertyTypeID>({0, 1, 2, 3})
        .expectVector<std::string_view>({"id", "changeid", "committed", "cheeky"})
        .expectVector<std::string_view>({"Int64", "String", "Bool", "Bool"})
        .execute();
}

TEST_F(QueryTest, ChangeQueryErrors) {
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("CHANGE NEW")
        .expectVector<const Change*>({}, false)
        .execute();

    const auto changes = tester.query("CHANGE LIST")
                             .expectVector<const Change*>({}, false)
                             .execute()
                             .outputColumnVector<const Change*>(0);

    ASSERT_TRUE(changes);

    tester.setChangeID(changes.value()->back()->id());

    tester.query("CREATE (n)")
        .expectError() // Requires label
        .execute();

    tester.query("CREATE (n:Person)-[e]-(m:Person)")
        .expectError() // Requires edge type
        .execute();
}

TEST_F(QueryTest, CallGraphInfo) {
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("CALL PROPERTIES()")
        .expectVector<PropertyTypeID>({0, 1, 2, 3, 4, 5, 6, 7})
        .expectVector<std::string_view>({"name", "dob", "age", "isFrench", "hasPhD", "isReal", "duration", "proficiency"})
        .expectVector<std::string_view>({"String", "String", "Int64", "Bool", "Bool", "Bool", "Int64", "String"})
        .execute();

    tester.query("CALL LABELS()")
        .expectVector<LabelID>({0, 1, 2, 3, 4, 5, 6, 7})
        .expectVector<std::string_view>({"Person", "SoftwareEngineering", "Founder", "Bioinformatics", "Interest", "Exotic", "Supernatural", "SleepDisturber"})
        .execute();

    tester.query("CALL EDGETYPES()")
        .expectVector<EdgeTypeID>({0, 1})
        .expectVector<std::string_view>({"KNOWS_WELL", "INTERESTED_IN"})
        .execute();

    tester.query("CALL LABELSETS()")
        .expectVector<LabelSetID>({0, 0, 0, 1, 1, 1, 2, 2, 3, 3, 4, 5, 5, 5, 5, 6, 6, 7, 7, 8, 8})
        .expectVector<std::string_view>({"Person", "SoftwareEngineering", "Founder", "Person", "Founder", "Bioinformatics", "SoftwareEngineering", "Interest", "Interest", "Exotic", "Interest", "Interest", "Exotic", "Supernatural", "SleepDisturber", "Person", "Bioinformatics", "Person", "SoftwareEngineering", "Interest", "SleepDisturber"})
        .execute();
}

TEST_F(QueryTest, InjectNodes) {
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("MATCH (n@0,1,8,9,11) RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Remy", "Adam", "Maxime", "Luc", "Martina"})
        .execute();

    tester.query("MATCH (n@0,1,8,9,11)--(m) return n.name,m.name")
        .expectOptVector<types::String::Primitive>({
            "Remy",
            "Remy",
            "Remy",
            "Remy",
            "Adam",
            "Adam",
            "Adam",
            "Maxime",
            "Maxime",
            "Luc",
            "Luc",
            "Martina",
        })
        .expectOptVector<types::String::Primitive>({
            "Adam",
            "Ghosts",
            "Computers",
            "Eighties",
            "Remy",
            "Bio",
            "Cooking",
            "Bio",
            "Paddle",
            "Animals",
            "Computers",
            "Cooking",
        });

    tester.query("MATCH (n:Founder@0,1,8,9,11) RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Remy", "Adam"})
        .execute();

    tester.query("MATCH (n:Founder,SoftwareEngineering@0,1,8,9,11) RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Remy"})
        .execute();

    tester.query("MATCH (n{hasPhD:False}@0,1,8,9,11) RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Maxime"})
        .execute();

    tester.query("MATCH (n{hasPhD:True,isFrench:True}@0,1,8,9,11) RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Remy", "Adam", "Luc"})
        .execute();

    tester.query("MATCH (n:Founder@0,1,8,9,11{hasPhD:True,isFrench:True}) RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Remy", "Adam"})
        .execute();

    tester.query("MATCH (n:Founder,Bioinformatics@0,1,8,9,11{hasPhD:True, isFrench:True}) RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Adam"})
        .execute();

    tester.query("MATCH (n@1001) RETURN n")
        .expectError()
        .execute();

    tester.query("MATCH (m)--(n@0) RETURN n")
        .expectError()
        .execute();
}

TEST_F(QueryTest, injectNodesCreate) {
    QueryTester tester {_env->getMem(), *_interp};

    const auto change0res = tester.query("CHANGE NEW")
                                .expectVector<const Change*>({}, false)
                                .execute()
                                .outputColumnVector<const Change*>(0);
    const ChangeID change0 = change0res.value()->back()->id();
    tester.setChangeID(change0);

    // Ensure nodes cannot be create with a specifc ID
    // Non-existant ID
    tester.query("create (n @ 333)")
        .expectError()
        .expectErrorMessage("Cannot create a node with a specific ID")
        .execute();

    // Existant ID
    tester.query("create (n @ 0)")
        .expectError()
        .expectErrorMessage("Cannot create a node with a specific ID")
        .execute();

    tester.query("change submit").execute();
    tester.setChangeID(ChangeID::head());

    const auto change1res = tester.query("CHANGE NEW")
                                .expectVector<const Change*>({}, false)
                                .execute()
                                .outputColumnVector<const Change*>(0);
    const ChangeID change1 = change1res.value()->back()->id();
    tester.setChangeID(change1);

    tester.query("create (a @ 0, 1)-[:BADEDGE]-(b @ 2)")
        .expectError()
        .expectErrorMessage(
            "Edges may only be created between nodes with at most one specified ID")
        .execute();

    tester.query("create (a @ 0)-[:BADEDGE]-(b @ 1, 2)")
        .expectError()
        .expectErrorMessage(
            "Edges may only be created between nodes with at most one specified ID")
        .execute();


    tester.query("create (a @ 0,1)-[:BADEDGE]-(b @ 2, 3)")
        .expectError()
        .expectErrorMessage(
            "Edges may only be created between nodes with at most one specified ID")
        .execute();

    tester.query("create (a @ 0,1)-[:BADEDGE]-(b @ 2)-[:GOODEDGE]-(c @ 3)")
        .expectError()
        .expectErrorMessage(
            "Edges may only be created between nodes with at most one specified ID")
        .execute();

    tester.query("create (a @ 0)-[:BADEDGE]-(b @ 2, 3)-[:BADEDGE]-(c @ 3)")
        .expectError()
        .expectErrorMessage(
            "Edges may only be created between nodes with at most one specified ID")
        .execute();

    tester.query("create (a @ 0)-[:GOODEDGE]-(b @ 1)-[:BADEDGE]-(c @ 2, 3)")
        .expectError()
        .expectErrorMessage(
            "Edges may only be created between nodes with at most one specified ID")
        .execute();

    tester.query("create (a @ 0, 1)-[:BADEDGE]-(b @ 2)-[:BADEDGE]-(c @ 3, 4)")
        .expectError()
        .expectErrorMessage(
            "Edges may only be created between nodes with at most one specified ID")
        .execute();

    // Standard simpledb Edges
    tester.query("match (n)-[e]-(m) return e")
        .expectVector<EdgeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .execute();
    tester.query("CALL EDGETYPES()")
        .expectVector<EdgeTypeID>({0, 1})
        .expectVector<std::string_view>({"KNOWS_WELL", "INTERESTED_IN"})
        .execute();

    // 1 hop valid edge
    tester.query("create (a @ 0)-[:GOODEDGE]-(b @ 1)")
        .execute();
    // 2 hop valid edge
    tester.query("create (a @ 0)-[:GOODEDGE]-(b @ 1)-[:GOODEDGE]-(c @ 2)")
        .execute();

    tester.query("change submit").execute();
    tester.setChangeID(ChangeID::head());

    // Standard simpledb Edges + GOODEDGE
    tester.query("match (n)-[e]-(m) return e") // We created edges ID 13-15
        .expectVector<EdgeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
        .execute();

    tester.query("CALL EDGETYPES()")
        .expectVector<EdgeTypeID>({0, 1, 2})
        .expectVector<std::string_view>({"KNOWS_WELL", "INTERESTED_IN", "GOODEDGE"})
        .execute();

    // Standard simpledb Nodes
    tester.query("match (n) return n")
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .execute();
}

TEST_F(QueryTest, stringApproxTest) {
    QueryTester tester {_env->getMem(), *_interp};

    auto change1Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);

    const ChangeID change1 = change1Res.value()->back()->id();

    tester.setChangeID(change1);

    const std::string createQuery = "create (n:NewNode{name=\"Norbert "
                                    "Norman\"})-[e:NewEdge{name=\"Norbert->Micheal\"}]-("
                                    "m:NewNode{name=\"Micheal Mann\"})";

    const std::string sanityCheck = "match (n:NewNode{name=\"Norbert "
                                    "Norman\"})-[e:NewEdge{name=\"Norbert->Micheal\"}]-("
                                    "m:NewNode{name=\"Micheal Mann\"}) return e.name";

    tester.query(createQuery).execute();
    tester.query("CHANGE SUBMIT").execute();
    tester.setChangeID(ChangeID::head());

    using StrOpt = std::optional<types::String::Primitive>;

    tester.query(sanityCheck)
        .expectVector<StrOpt>({"Norbert->Micheal"}) // Ensure the edge is created
        .execute();

    // Strict node contraints, approx on edge name
    const std::string approxEdgeNameNorbert =
        "match (n:NewNode{name=\"Norbert "
        "Norman\"})-[e:NewEdge{name~=\"Norbert\"}]-("
        "m:NewNode{name=\"Micheal Mann\"}) return e.name";
    const std::string approxEdgeNameMicheal =
        "match (n:NewNode{name=\"Norbert "
        "Norman\"})-[e:NewEdge{name~=\"Micheal\"}]-("
        "m:NewNode{name=\"Micheal Mann\"}) return e.name";

    tester.query(approxEdgeNameNorbert)
        .expectVector<StrOpt>({"Norbert->Micheal"})
        .execute();
    tester.query(approxEdgeNameMicheal)
        .expectVector<StrOpt>({"Norbert->Micheal"})
        .execute();


    // No name constraints on nodes
    const std::string approxEdgeNameNorbertSlim =
        "match (n:NewNode"
        ")-[e:NewEdge{name~=\"Norbert\"}]-("
        "m:NewNode) return e.name";
    const std::string approxEdgeNameMichealSlim =
        "match (n:NewNode"
        ")-[e:NewEdge{name~=\"Micheal\"}]-("
        "m:NewNode) return e.name";

    tester.query(approxEdgeNameNorbertSlim)
        .expectVector<StrOpt>({"Norbert->Micheal"})
        .execute();
    tester.query(approxEdgeNameMichealSlim)
        .expectVector<StrOpt>({"Norbert->Micheal"})
        .execute();

    // Approximate constraints on nodes and edge
    const std::string approxEdgeNameApproxNorbert =
        "match (n:NewNode{name~=\"Norbert "
        "\"})-[e:NewEdge{name~=\"Norbert\"}]-("
        "m:NewNode) return e.name";

    const std::string approxEdgeNameApproxMicheal =
        "match (n:NewNode"
        ")-[e:NewEdge{name~=\"Micheal\"}]-("
        "m:NewNode{name~=\"Micheal\"}) return e.name";

    tester.query(approxEdgeNameApproxNorbert)
        .expectVector<StrOpt>({"Norbert->Micheal"})
        .execute();
    tester.query(approxEdgeNameApproxMicheal)
        .expectVector<StrOpt>({"Norbert->Micheal"})
        .execute();

    const std::string allApproxNoLabels =
        "match (n{name~=\"Norbert\"}"
        ")-[e{name~=\"Micheal\"}]-("
        "m{name~=\"Micheal\"}) return e.name";

    tester.query(allApproxNoLabels)
        .expectVector<StrOpt>({"Norbert->Micheal"})
        .execute();
}

TEST_F(QueryTest, StringAproxMultiExpr) {
    QueryTester tester {_env->getMem(), *_interp};

    using StrOpt = std::optional<types::String::Primitive>;

    const std::string createQuery = "create (n:NewNode{poem=\"the cat jumped\", rating=5u})";
    const auto change1Res = tester.query("change new")
                                .expectVector<const Change*>({}, false)
                                .execute()
                                .outputColumnVector<const Change*>(0);

    const ChangeID change1 = change1Res.value()->back()->id();

    tester.setChangeID(change1);

    tester.query(createQuery).execute();
    tester.query("CHANGE SUBMIT").execute();
    tester.setChangeID(ChangeID::head());


    const std::string matchQuery = "match (n:NewNode{poem~=\"cat\", rating=5u}) return n.poem";


    tester.query(matchQuery)
        .expectVector<StrOpt>({"the cat jumped"})
        .execute();
}

TEST_F(QueryTest, personGraphApproxMatching) {
    QueryTester tester {_env->getMem(), *_interp};

    using StrOpt = std::optional<types::String::Primitive>;

    const auto changeRes = tester.query("change new")
                               .expectVector<const Change*>({}, false)
                               .execute()
                               .outputColumnVector<const Change*>(0);

    const ChangeID change = changeRes.value()->back()->id();
    tester.setChangeID(change);

    tester.query(
              R"(create (n:Person{name="Cyrus", hasPhD=false})-[e:Edgey{name="Housemate"}]-(m:Person{name="Sai", hasPhD=true}))")
        .execute();

    // Commit the change
    tester.query("change submit").execute();
    tester.setChangeID(ChangeID::head());

    // Run the match queries
    tester.query(R"(match (n:Person{name="Cyrus"}) return n.name)")
        .expectVector<StrOpt>({"Cyrus"})
        .execute();

    tester.query(R"(match (n:Person{name~="Cyrus"}) return n.name)")
        .expectVector<StrOpt>({"Cyrus"})
        .execute();

    tester.query(R"(match (n:Person{name~="Cyrus", hasPhD=false}) return n.name)")
        .expectVector<StrOpt>({"Cyrus"})
        .execute();

    tester.query(R"(match (n{name="Cyrus"})-[e{name="Housemate"}]-(m{name="Sai"}) return e.name)")
        .expectVector<StrOpt>({"Housemate"})
        .execute();

    tester.query(R"(match (n{name~="Cy"})-[e{name~="House"}]-(m{name~="Sa"}) return e.name)")
        .expectVector<StrOpt>({"Housemate"})
        .execute();

    tester.query(R"(match (n{name~="Cy", hasPhD=false})-[e{name~="House"}]-(m{name~="Sa"}) return e.name)")
        .expectVector<StrOpt>({"Housemate"})
        .execute();

    tester.query(R"(match (n:Person{name~="Cy", hasPhD=false})-[e:Edgey{name~="House"}]-(m:Person{name~="Sa"}) return e.name)")
        .expectVector<StrOpt>({"Housemate"})
        .execute();
}

TEST_F(QueryTest, createChunkingTest) {
    QueryTester tester {_env->getMem(), *_interp};

    auto change1Res = tester.query("CHANGE NEW")
                          .expectVector<const Change*>({}, false)
                          .execute()
                          .outputColumnVector<const Change*>(0);
    ASSERT_TRUE(change1Res);

    const ChangeID change1 = change1Res.value()->back()->id();

    tester.setChangeID(change1);

    tester.query("create (n:Z1)")
        .execute();

    tester.query("create (n:A1)-[:AE1]-(m:A2)")
        .execute();

    tester.query("create (n:B1)-[:BE1]-(m:B2)-[:BE2]-(o:B3)")
        .execute();

    tester.query("create (n:C1)-[:CE1]-(m:C2)-[:CE2]-(o:C3)-[:CE3]-(p:C4)")
        .execute();

    tester.query("create (n:D1)-[:DE1]-(m:D2)-[:DE2]-(o:D3)-[:DE3]-(p:D4)-[:DE4]-(q:D5)")
        .execute();

    tester.query("change submit")
        .execute();

    tester.setChangeID(ChangeID::head());

    NodeID nextNodeID = 13;
    EdgeID nextEdgeID = 13;

    tester.query("match (n:Z1) return n")
        .expectVector<NodeID>({nextNodeID++})
        .execute();

    tester.query("match (:A1)-[e:AE1]-(:A2) return e")
        .expectVector<EdgeID>({nextEdgeID++})
        .execute();

    tester.query("match (:B1)-[e:BE1]-(:B2)-[f:BE2]-(:B3) return e, f")
        .expectVector<EdgeID>({nextEdgeID++})
        .expectVector<EdgeID>({nextEdgeID++})
        .execute();

    tester.query("match (:C1)-[e:CE1]-(:C2)-[f:CE2]-(:C3)-[g:CE3]-(:C4) return e, f, g")
        .expectVector<EdgeID>({nextEdgeID++})
        .expectVector<EdgeID>({nextEdgeID++})
        .expectVector<EdgeID>({nextEdgeID++})
        .execute();

    tester.query("match (:D1)-[e:DE1]-(:D2)-[f:DE2]-(:D3)-[g:DE3]-(:D4)-[h:DE4]-(:D5) return e, f, g, h")
        .expectVector<EdgeID>({nextEdgeID++})
        .expectVector<EdgeID>({nextEdgeID++})
        .expectVector<EdgeID>({nextEdgeID++})
        .expectVector<EdgeID>({nextEdgeID++})
        .execute();
}
