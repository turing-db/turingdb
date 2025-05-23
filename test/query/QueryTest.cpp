#include <gtest/gtest.h>

#include "LocalMemory.h"
#include "versioning/Change.h"
#include "QueryInterpreter.h"
#include "TuringDB.h"
#include "SimpleGraph.h"
#include "QueryTester.h"
#include "TuringTest.h"

using namespace db;

class QueryTest : public turing::test::TuringTest {
public:
    void initialize() override {
        SystemManager& sysMan = _db.getSystemManager();
        Graph* graph = sysMan.createGraph("simple");
        SimpleGraph::createSimpleGraph(graph);
        _interp = std::make_unique<QueryInterpreter>(&_db.getSystemManager(),
                                                     &_db.getJobSystem());
    }

protected:
    TuringDB _db;
    LocalMemory _mem;
    std::unique_ptr<QueryInterpreter> _interp {nullptr};
};

TEST_F(QueryTest, ReturnNode) {
    QueryTester tester {_mem, *_interp};

    tester.query("MATCH n RETURN n")
        .expectVector<EntityID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .execute();


    tester.query("MATCH n-[e]-m RETURN e")
        .expectVector<EntityID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .execute();

    tester.query("MATCH n-[e]-m RETURN n")
        .expectVector<EntityID>({0, 0, 0, 0, 1, 1, 1, 6, 8, 8, 9, 9, 11})
        .execute();

    tester.query("MATCH n-[e]-m RETURN m")
        .expectVector<EntityID>({1, 6, 2, 3, 0, 4, 5, 0, 4, 7, 10, 2, 5})
        .execute();

    tester.query("MATCH n-[e]-m RETURN n, m")
        .expectVector<EntityID>({0, 0, 0, 0, 1, 1, 1, 6, 8, 8, 9, 9, 11})
        .expectVector<EntityID>({1, 6, 2, 3, 0, 4, 5, 0, 4, 7, 10, 2, 5})
        .execute();

    tester.query("MATCH n-[e]-m RETURN n, e, m")
        .expectVector<EntityID>({0, 0, 0, 0, 1, 1, 1, 6, 8, 8, 9, 9, 11})
        .expectVector<EntityID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .expectVector<EntityID>({1, 6, 2, 3, 0, 4, 5, 0, 4, 7, 10, 2, 5})
        .execute();

    tester.query("MATCH n--m--q--r RETURN n, m, q, r")
        .expectVector<EntityID>({0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 6, 6, 6, 6})
        .expectVector<EntityID>({1, 1, 1, 1, 6, 6, 6, 6, 0, 0, 0, 0, 0, 0, 0, 0})
        .expectVector<EntityID>({0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 6, 1, 1, 1, 6})
        .expectVector<EntityID>({1, 6, 2, 3, 1, 6, 2, 3, 0, 4, 5, 0, 0, 4, 5, 0})
        .execute();
}

TEST_F(QueryTest, EdgeMatching) {
    QueryTester tester {_mem, *_interp};

    tester.query("MATCH n-[e]-m RETURN n, e, m")
        .expectVector<EntityID>({0, 0, 0, 0, 1, 1, 1, 6, 8, 8, 9, 9, 11})
        .expectVector<EntityID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .expectVector<EntityID>({1, 6, 2, 3, 0, 4, 5, 0, 4, 7, 10, 2, 5})
        .execute();

    tester.query("MATCH n-[e]-m-[e1]-q-[e2]-r RETURN n, e, m, e1, q, e2, r")
        .expectVector<EntityID>({0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 6, 6, 6, 6})
        .expectVector<EntityID>({0, 0, 0, 0, 1, 1, 1, 1, 4, 4, 4, 4, 7, 7, 7, 7})
        .expectVector<EntityID>({1, 1, 1, 1, 6, 6, 6, 6, 0, 0, 0, 0, 0, 0, 0, 0})
        .expectVector<EntityID>({4, 4, 4, 4, 7, 7, 7, 7, 0, 0, 0, 1, 0, 0, 0, 1})
        .expectVector<EntityID>({0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 6, 1, 1, 1, 6})
        .expectVector<EntityID>({0, 1, 2, 3, 0, 1, 2, 3, 4, 5, 6, 7, 4, 5, 6, 7})
        .expectVector<EntityID>({1, 6, 2, 3, 1, 6, 2, 3, 0, 4, 5, 0, 0, 4, 5, 0})
        .execute();


    tester.query("MATCH n:Person-[e]-m:Interest RETURN n, n.name, e.name, m.name, m")
        .expectVector<EntityID>({0, 0, 0, 1, 1, 8, 8, 9, 9, 11})
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
        .expectVector<EntityID>({6, 2, 3, 4, 5, 4, 7, 10, 2, 5})
        .execute();
}

TEST_F(QueryTest, MatchWildcard) {
    QueryTester tester {_mem, *_interp};
    tester.query("MATCH n RETURN *")
        .expectVector<EntityID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .execute();

    tester.query("MATCH n--m RETURN *")
        .expectVector<EntityID>({0, 0, 0, 0, 1, 1, 1, 6, 8, 8, 9, 9, 11})
        .expectVector<EntityID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .expectVector<EntityID>({1, 6, 2, 3, 0, 4, 5, 0, 4, 7, 10, 2, 5})
        .execute();

    tester.query("MATCH n:Person--m:Person RETURN n.name, *")
        .expectOptVector<types::String::Primitive>({"Remy", "Adam"})
        .expectVector<EntityID>({0, 1})
        .expectVector<EntityID>({0, 4})
        .expectVector<EntityID>({1, 0})
        .execute();
}

TEST_F(QueryTest, MatchNodeLabel) {
    QueryTester tester {_mem, *_interp};

    tester.query("MATCH n:Person RETURN n")
        .expectVector<EntityID>({0, 1, 8, 9, 11, 12})
        .execute();

    tester.query("MATCH n:Interest RETURN n")
        .expectVector<EntityID>({2, 3, 4, 5, 6, 7, 10})
        .execute();

    tester.query("MATCH n:Founder RETURN n, n.name")
        .expectVector<EntityID>({0, 1})
        .expectOptVector<types::String::Primitive>({"Remy", "Adam"})
        .execute();

    tester.query("MATCH n:Founder,SoftwareEngineering RETURN n")
        .expectVector<EntityID>({0})
        .execute();

    tester.query("MATCH n:SoftwareEngineering RETURN n")
        .expectVector<EntityID>({0, 2, 9, 12})
        .execute();
}

TEST_F(QueryTest, MatchEdgeType) {
    QueryTester tester {_mem, *_interp};

    tester.query("MATCH n-[e:INTERESTED_IN]-m RETURN n, e, m")
        .expectVector<EntityID>({0, 0, 0, 1, 1, 8, 8, 9, 9, 11})
        .expectVector<EntityID>({1, 2, 3, 5, 6, 8, 9, 10, 11, 12})
        .expectVector<EntityID>({6, 2, 3, 4, 5, 4, 7, 10, 2, 5})
        .execute();

    tester.query("MATCH n-[e:KNOWS_WELL]-m RETURN n, n.name, e, e.name, m, m.name")
        .expectVector<EntityID>({0, 1, 6})
        .expectOptVector<types::String::Primitive>({
            "Remy",
            "Adam",
            "Ghosts",
        })
        .expectVector<EntityID>({0, 4, 7})
        .expectOptVector<types::String::Primitive>({
            "Remy -> Adam",
            "Adam -> Remy",
            "Ghosts -> Remy",
        })
        .expectVector<EntityID>({1, 0, 0})
        .expectOptVector<types::String::Primitive>({
            "Adam",
            "Remy",
            "Remy",
        })
        .execute();

    tester.query("MATCH n-[e:DOES_NOT_EXIST]-m RETURN n, e, m")
        .expectVector<EntityID>({})
        .expectVector<EntityID>({})
        .expectVector<EntityID>({})
        .execute();
}

TEST_F(QueryTest, NodePropertyProjection) {
    QueryTester tester {_mem, *_interp};

    tester.query("MATCH n RETURN n, n.name")
        .expectVector<EntityID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
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

    tester.query("MATCH n:Person RETURN n, n.name")
        .expectVector<EntityID>({0, 1, 8, 9, 11, 12})
        .expectOptVector<types::String::Primitive>({"Remy", "Adam", "Maxime", "Luc", "Martina", "Suhas"})
        .execute();

    tester.query("MATCH n:Interest RETURN n, n.name")
        .expectVector<EntityID>({2, 3, 4, 5, 6, 7, 10})
        .expectOptVector<types::String::Primitive>({"Computers", "Eighties", "Bio", "Cooking", "Ghosts", "Paddle", "Animals"})
        .execute();

    tester.query("MATCH n:Founder RETURN n, n.name")
        .expectVector<EntityID>({0, 1})
        .expectOptVector<types::String::Primitive>({"Remy", "Adam"})
        .execute();

    tester.query("MATCH n:Founder,SoftwareEngineering RETURN n, n.name")
        .expectVector<EntityID>({0})
        .expectOptVector<types::String::Primitive>({"Remy"})
        .execute();

    tester.query("MATCH n:SoftwareEngineering RETURN n, n.name")
        .expectVector<EntityID>({0, 2, 9, 12})
        .expectOptVector<types::String::Primitive>({"Remy", "Computers", "Luc", "Suhas"})
        .execute();

    tester.query("MATCH n RETURN n.doesnotexist")
        .expectError()
        .execute();
}

TEST_F(QueryTest, EdgePropertyProjection) {
    QueryTester tester {_mem, *_interp};

    tester.query("MATCH n-[e]-m RETURN e.name")
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

    tester.query("MATCH n:Person--m:Interest RETURN n, n.name, m, m.name")
        .expectVector<EntityID>({0, 0, 0, 1, 1, 8, 8, 9, 9, 11})
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
        .expectVector<EntityID>({6, 2, 3, 4, 5, 4, 7, 10, 2, 5})
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

    tester.query("MATCH n-[e]-m RETURN e.doesnotexist")
        .expectError();

    tester.query("MATCH n-[e:INTERESTED_IN]-m RETURN n, n.name, e, e.name, m, m.name")
        .expectVector<EntityID>({0, 0, 0, 1, 1, 8, 8, 9, 9, 11})
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
        .expectVector<EntityID>({1, 2, 3, 5, 6, 8, 9, 10, 11, 12})
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
        .expectVector<EntityID>({6, 2, 3, 4, 5, 4, 7, 10, 2, 5})
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

    tester.query("MATCH n-[e]-m RETURN e.doesnotexist")
        .expectError();
}

TEST_F(QueryTest, PropertyConstraints) {
    QueryTester tester {_mem, *_interp};

    tester.query("MATCH n{hasPhD:true} RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Remy", "Adam", "Luc", "Martina"})
        .execute();

    tester.query("MATCH n{hasPhD:true, isFrench: false} RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Martina"})
        .execute();

    tester.query("MATCH n:SoftwareEngineering{hasPhD:false} RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Suhas"})
        .execute();

    tester.query("MATCH n:Person,SoftwareEngineering{hasPhD:false, isFrench: false} RETURN n.name")
        .expectOptVector<types::String::Primitive>({"Suhas"})
        .execute();

    tester.query("MATCH n-[e{proficiency:\"expert\"}]-m RETURN n.name,m.name")
        .expectOptVector<types::String::Primitive>({"Remy", "Remy", "Ghosts", "Maxime"})
        .expectOptVector<types::String::Primitive>({"Ghosts", "Computers", "Remy", "Paddle"})
        .execute();

    tester.query("MATCH n-[e{proficiency:\"expert\", duration:20}]-m RETURN n.name,m.name")
        .expectOptVector<types::String::Primitive>({"Remy"})
        .expectOptVector<types::String::Primitive>({"Ghosts"})
        .execute();

    tester.query("MATCH n-[e{proficiency:\"expert\"}]-m{isReal:true} RETURN n.name,m.name")
        .expectOptVector<types::String::Primitive>({"Remy", "Remy"})
        .expectOptVector<types::String::Primitive>({"Ghosts", "Computers"})
        .execute();

    tester.query("MATCH n-[e{proficiency:\"expert\"}]-m:SoftwareEngineering{isReal:true} RETURN n.name,m.name")
        .expectOptVector<types::String::Primitive>({"Remy"})
        .expectOptVector<types::String::Primitive>({"Computers"})
        .execute();

    tester.query("MATCH n-[e:KNOWS_WELL{duration:20}]-m:SoftwareEngineering RETURN n.name,m.name")
        .expectOptVector<types::String::Primitive>({"Adam"})
        .expectOptVector<types::String::Primitive>({"Remy"})
        .execute();

    tester.query("MATCH n-[e:INTERESTED_IN{duration:20}]-m:Exotic{isReal:true} RETURN n.name,m.name")
        .expectOptVector<types::String::Primitive>({"Remy"})
        .expectOptVector<types::String::Primitive>({"Ghosts"})
        .execute();
}

TEST_F(QueryTest, ChangeQuery) {
    QueryTester tester {_mem, *_interp};

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

    tester.query("CREATE (n:9)-[e:INTERESTED_IN { name: \"Luc -> Video games\" }]-(m:Interest { name: \"Video games\" })")
        .execute();

    tester.query("COMMIT").execute();

    tester.query("MATCH n:Person RETURN n.name")
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

    tester.query("MATCH n:Interest RETURN n.name")
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

    tester.query("MATCH n-[e]-m RETURN e.name")
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

    tester.query("MATCH n-[e:NEW_EDGE_TYPE]-m RETURN e.name")
        .expectOptVector<types::String::Primitive>({"New edge"})
        .execute();

    tester.query("CHANGE SUBMIT")
        .execute();

    tester.setChangeID(ChangeID::head());
    tester.setCommitHash(CommitHash::head());

    tester.query("MATCH n:Person RETURN n.name")
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
    QueryTester tester {_mem, *_interp};

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
    QueryTester tester {_mem, *_interp, "default"};

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
        .expectVector<EntityID>({0, 1})
        .expectOptVector<types::String::Primitive>({"Luc", "Remy"})
        .execute();
}

TEST_F(QueryTest, ChangeQueryErrors) {
    QueryTester tester {_mem, *_interp};

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
