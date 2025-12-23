#include <gtest/gtest.h>

#include <initializer_list>
#include <string_view>
#include <cstdint>

#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "versioning/Change.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "ID.h"
#include "versioning/ChangeID.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"

#include "LineContainer.h"
#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class ChangeQueriesTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    ChangeID _currentChange {ChangeID::head()};

    static constexpr std::string_view GET_PROPERTIES_QUERY =
        "CALL db.propertyTypes() YIELD propertyType as property";

    static constexpr auto emptyCallback = [](const Dataframe*) -> void {};

    GraphReader read() { return _graph->openTransaction().readGraph(); }

    void newChange() {
        auto res = _env->getSystemManager().newChange(_graphName);
        ASSERT_TRUE(res);

        Change* change = res.value();
        _currentChange = change->id();
    }

    void submitCurrentChange() {
        auto res = _db->queryV2("CHANGE SUBMIT", _graphName, &_env->getMem(),
                                emptyCallback, CommitHash::head(), _currentChange);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    void submitChange(ChangeID chid) {
        auto res = _db->queryV2("CHANGE SUBMIT", _graphName, &_env->getMem(),
                                emptyCallback, CommitHash::head(), chid);

        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    void setChange(ChangeID chid) {
        _currentChange = chid;
    }

    auto query(std::string_view query, auto callback) {
        auto res = _db->queryV2(query, _graphName, &_env->getMem(), callback,
                                CommitHash::head(), _currentChange);
        return res;
    }

    void setWorkingGraph(std::string_view name) {
        _graphName = name;
        _graph = _env->getSystemManager().getGraph(std::string {name});
        ASSERT_TRUE(_graph);
    }

    static NamedColumn* findColumn(const Dataframe* df, std::string_view name) {
        for (auto* col : df->cols()) {
            if (col->getName() == name) {
                return col;
            }
        }
        return nullptr;
    }

    // Helper to check the output of a db.propertyTypes()
    bool ensureProperties(std::initializer_list<std::string_view> props) {
        bool allPropsFound {true};

        auto res = query(GET_PROPERTIES_QUERY, [&](const Dataframe* df) {
            ASSERT_TRUE(df);

            NamedColumn* propCol = findColumn(df, "property");

            ASSERT_TRUE(propCol);

            auto* castedProps = propCol->as<ColumnVector<std::string_view>>();
            ASSERT_TRUE(castedProps);

            for (const auto& desired : props) {
                bool found {false};
                for (const auto& prop : *castedProps) {
                    if (prop == desired) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    allPropsFound = false;
                    return;
                }
            }
        });

        EXPECT_TRUE(res);
        return allPropsFound;
    }
};

TEST_F(ChangeQueriesTest, changeWithRebaseQueries) {
    ChangeID change1;
    ChangeID change2;
    {
        newChange(), change1 = _currentChange;
        std::string_view CREATE_QUERY =
            R"(CREATE (n:TestNode1 { name: "1" })-[e:TestEdge1 { name: "1->2" }]->(m:TestNode1 { name: "2" }))";
        ASSERT_TRUE(query(CREATE_QUERY, emptyCallback));
    }
    {
        newChange(), change2 = _currentChange;
        std::string_view CREATE_QUERY =
            R"(CREATE (n:TestNode2 { name: "3" })-[e:TestEdge2 { name: "3->4" }]->(m:TestNode2 { name: "4" }))";
        ASSERT_TRUE(query(CREATE_QUERY, emptyCallback));
    }

    submitChange(change2);
    submitChange(change1);

    using Name = types::String::Primitive;
    using Rows = LineContainer<Name, Name, Name>;

    {
        Rows expected;
        expected.add({"1", "1->2", "2"});

        std::string_view MATCH_QUERY = "MATCH (n:TestNode1)-[e:TestEdge1]->(m:TestNode1) "
                                       "RETURN n.name, e.name, m.name";
        Rows actual;
        auto res = query(MATCH_QUERY, [&actual](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(3, df->size());

            auto* nname = findColumn(df, "n.name")->as<ColumnOptVector<Name>>();
            auto* mname = findColumn(df, "m.name")->as<ColumnOptVector<Name>>();
            auto* ename = findColumn(df, "e.name")->as<ColumnOptVector<Name>>();

            ASSERT_TRUE(nname);
            ASSERT_TRUE(ename);
            ASSERT_TRUE(mname);

            ASSERT_EQ(1, nname->size());
            ASSERT_EQ(1, ename->size());
            ASSERT_EQ(1, mname->size());

            actual.add({*nname->front(), *ename->front(), *mname->front()});

        });
        ASSERT_TRUE(res);

        ASSERT_TRUE(expected.equals(actual));
    }

    {
        Rows expected;
        expected.add({"3", "3->4", "4"});

        std::string_view MATCH_QUERY = "MATCH (n:TestNode2)-[e:TestEdge2]->(m:TestNode2) "
                                       "RETURN n.name, e.name, m.name";
        Rows actual;
        auto res = query(MATCH_QUERY, [&actual](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(3, df->size());

            auto* nname = findColumn(df, "n.name")->as<ColumnOptVector<Name>>();
            auto* mname = findColumn(df, "m.name")->as<ColumnOptVector<Name>>();
            auto* ename = findColumn(df, "e.name")->as<ColumnOptVector<Name>>();

            ASSERT_TRUE(nname);
            ASSERT_TRUE(ename);
            ASSERT_TRUE(mname);

            ASSERT_EQ(1, nname->size());
            ASSERT_EQ(1, mname->size());
            ASSERT_EQ(1, ename->size());

            actual.add({*nname->front(), *ename->front(), *mname->front()});
        });
        ASSERT_TRUE(res);

        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(ChangeQueriesTest, threeChangeRebase) {
    setWorkingGraph("default");

    auto makeChanges = [&]() -> void {
        newChange();
        { // Change
            ASSERT_TRUE(query("CREATE (n:NODE)", emptyCallback));
            ASSERT_TRUE(query("COMMIT", emptyCallback));

            ASSERT_TRUE(query("MATCH (n:NODE) CREATE (n)-[:EDGE]->(m:NODE)", emptyCallback));
            ASSERT_TRUE(query("COMMIT", emptyCallback));
        }
    };

    makeChanges();
    ChangeID change1 = _currentChange;

    makeChanges();
    ChangeID change2 = _currentChange;

    makeChanges();
    ChangeID change3 = _currentChange;

    {
        submitChange(change3);
        submitChange(change2);
        submitChange(change1);
    }

    { // Verify nodes
        using Rows = LineContainer<NodeID>;
        Rows expected;
        size_t numChanges = 3;
        size_t nodesPerChange = 2;
        for (size_t n = 0; n < numChanges * nodesPerChange; n++) {
            expected.add({n});
        }

        Rows actual;
        {
            auto res = query("MATCH (n) RETURN n", [&actual](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(1, df->size());
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                for (auto & n : *ns) {
                    actual.add({n});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(actual));
    }

    { // Verify edges
        using Rows = LineContainer<NodeID, EdgeID, NodeID>;
        Rows expected;
        expected.add({0, 0, 1});
        expected.add({2, 1, 3});
        expected.add({4, 2, 5});

        Rows actual;
        {
            auto res = query("MATCH (n)-[e]->(m) RETURN n,e,m", [&actual](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(3, df->size());

                auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
                auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
                auto* ms = findColumn(df, "m")->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                ASSERT_TRUE(es);
                ASSERT_TRUE(ms);

                for (size_t row {0}; row < ns->size(); row++) {
                    actual.add({ns->at(row), es->at(row), ms->at(row)});
                }
            });
            ASSERT_TRUE(res);
        }

        actual.print(std::cout);
        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(ChangeQueriesTest, commitThenRebase) {
    setWorkingGraph("default");

    newChange();
    ChangeID change1 = _currentChange;

    { // Change 1 commits locally, does not submit
        ASSERT_TRUE(query(
            R"(create (n:CHANGE1LABEL {id:1, changeid: "ONE", committed:true}))",
            emptyCallback));

        ASSERT_TRUE(query("COMMIT", emptyCallback));

        ASSERT_TRUE(query(
            R"(create (n:CHANGE1LABEL {cheeky:true, id:2, changeid: "ONE", committed:true}))",
            emptyCallback));

        ASSERT_TRUE(query(
            R"(create (n:CHANGE1LABEL {cheeky:true, id:3, changeid: "ONE", committed:true}))",
            emptyCallback));

        ASSERT_TRUE(query("COMMIT", emptyCallback));
        ASSERT_TRUE(ensureProperties({"id", "changeid", "committed"}));
    }

    newChange();
    [[maybe_unused]] ChangeID change2 = _currentChange;

    { // Change 2
        ASSERT_TRUE(query(
            R"(create (n:CHANGE2LABEL {id:4, changeid: "TWO", committed:false}))",
            emptyCallback));
        ASSERT_TRUE(query(
            R"(create (n:CHANGE2LABEL {id:5, changeid: "TWO", committed:false}))",
            emptyCallback));
        submitCurrentChange();
    }

    { // Verify submission of Change 2
        std::string_view matchQuery = "MATCH (n) RETURN n, n.id, n.changeid, n.committed";

        using Rows = LineContainer<NodeID, int64_t, std::string_view, bool>;

        Rows expected;
        {
            expected.add({0, 4, "TWO", false});
            expected.add({1, 5, "TWO", false});
        }

        Rows actual;
        {
            auto res = query(matchQuery, [&actual](const Dataframe* df) {
                ASSERT_TRUE(df);
                auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
                auto* ids = findColumn(df, "n.id")
                                ->as<ColumnOptVector<types::Int64::Primitive>>();
                auto* chids = findColumn(df, "n.changeid")
                                  ->as<ColumnOptVector<types::String::Primitive>>();
                auto* cmts = findColumn(df, "n.committed")
                                 ->as<ColumnOptVector<types::Bool::Primitive>>();
                ASSERT_TRUE(ns);
                ASSERT_TRUE(ids);
                ASSERT_TRUE(chids);
                ASSERT_TRUE(cmts);

                for (size_t row {0}; row < df->getRowCount(); row++) {
                    actual.add(
                        {ns->at(row), *ids->at(row), *chids->at(row), *cmts->at(row)});
                }
            });
            ASSERT_TRUE(res);
        }
        EXPECT_TRUE(expected.equals(actual));

        EXPECT_TRUE(ensureProperties({"id", "changeid", "committed"}));
    }

    { // Submit Change 1 and verify submission
        submitChange(change1);

        std::string_view matchQuery = "MATCH (n) RETURN n, n.id, n.changeid, n.committed";
        using Rows = LineContainer<NodeID, int64_t, std::string_view, bool>;

        Rows expected;
        {
            expected.add({0, 4, "TWO", false});
            expected.add({1, 5, "TWO", false});
            expected.add({2, 1, "ONE", true});
            expected.add({3, 2, "ONE", true});
            expected.add({4, 3, "ONE", true});
        }

        Rows actual;
        {
            auto res = query(matchQuery, [&actual](const Dataframe* df) {
                ASSERT_TRUE(df);
                auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
                auto* ids = findColumn(df, "n.id")
                                ->as<ColumnOptVector<types::Int64::Primitive>>();
                auto* chids = findColumn(df, "n.changeid")
                                  ->as<ColumnOptVector<types::String::Primitive>>();
                auto* cmts = findColumn(df, "n.committed")
                                 ->as<ColumnOptVector<types::Bool::Primitive>>();
                ASSERT_TRUE(ns);
                ASSERT_TRUE(ids);
                ASSERT_TRUE(chids);
                ASSERT_TRUE(cmts);

                for (size_t row {0}; row < df->getRowCount(); row++) {
                    actual.add(
                        {ns->at(row), *ids->at(row), *chids->at(row), *cmts->at(row)});
                }
            });
            ASSERT_TRUE(res);
        }
        EXPECT_TRUE(expected.equals(actual));
        EXPECT_TRUE(ensureProperties({"id", "changeid", "committed", "cheeky"}));
    }
}

TEST_F(ChangeQueriesTest, deleteNodeConflict) {
    std::string_view deleteRemy = R"(MATCH (n) WHERE n.name = "Remy" DELETE n)";

    ChangeID change1;
    ChangeID change2;
    {
        newChange(), change1 = _currentChange;

        ASSERT_TRUE(query(deleteRemy, emptyCallback));
    }

    {
        newChange(), change2 = _currentChange;

        ASSERT_TRUE(query(deleteRemy, emptyCallback));
    }

    submitChange(change2);

    setChange(change1);
    auto res = query("CHANGE SUBMIT", emptyCallback);

    EXPECT_FALSE(res);
    ASSERT_TRUE(res.hasErrorMessage());

    std::string_view err = res.getError();
    std::string_view expectedErr = "This change attempted to delete Node 0 (which is now "
                                   "Node 0 on main) which has been modified on main.";

    EXPECT_EQ(expectedErr, err);
}

TEST_F(ChangeQueriesTest, deleteEdgeConflict) {
    std::string_view deleteCyrusTravel =
        R"(MATCH (n)-[e]->(m) WHERE n.name = "Cyrus" AND m.name = "Travel" DELETE e)";

    ChangeID change1;
    ChangeID change2;
    {
        newChange(), change1 = _currentChange;

        ASSERT_TRUE(query(deleteCyrusTravel, emptyCallback));
    }

    {
        newChange(), change2 = _currentChange;

        ASSERT_TRUE(query(deleteCyrusTravel, emptyCallback));
    }

    submitChange(change2);

    setChange(change1);
    auto res = query("CHANGE SUBMIT", emptyCallback);

    EXPECT_FALSE(res);
    ASSERT_TRUE(res.hasErrorMessage());

    std::string_view err = res.getError();
    std::string_view expectedErr =
        "This change attempted to delete Edge 14 (which is now "
        "Edge 14 on main) which has been modified on main.";

    EXPECT_EQ(expectedErr, err);
}

TEST_F(ChangeQueriesTest, deleteOutEdgeSideEffect) {
    std::string_view createSuhasCyrus =
        R"(MATCH (n), (m) WHERE n.name = "Suhas" AND m.name = "Cyrus" CREATE (n)-[:WORKS_WITH]->(m))";
    std::string_view deleteSuhas = R"(MATCH (n) WHERE n.name = "Suhas" DELETE n)";

    std::string_view expectedError =
        "Submit rejected: Commits on main have created an edge (ID: 18) incident to Node "
        "12, which this Change attempts to delete.";

    ChangeID change1, change2;

    {
        newChange(), change1 = _currentChange;

        ASSERT_TRUE(query(createSuhasCyrus, emptyCallback));
    }

    {
        newChange(), change2 = _currentChange;

        ASSERT_TRUE(query(deleteSuhas, emptyCallback));
    }

    submitChange(change1);

    setChange(change2);

    auto res = query("CHANGE SUBMIT", emptyCallback);

    EXPECT_FALSE(res);
    ASSERT_TRUE(res.hasErrorMessage());
    EXPECT_EQ(expectedError, res.getError());
}

TEST_F(ChangeQueriesTest, deleteInEdgeSideEffect) {
    std::string_view createCyrusSuhas =
        R"(MATCH (n), (m) WHERE n.name = "Suhas" AND m.name = "Cyrus" CREATE (m)-[:WORKS_WITH]->(n))";
    std::string_view deleteSuhas = R"(MATCH (n) WHERE n.name = "Suhas" DELETE n)";

    std::string_view expectedError =
        "Submit rejected: Commits on main have created an edge (ID: 18) incident to Node "
        "12, which this Change attempts to delete.";

    ChangeID change1, change2;

    {
        newChange(), change1 = _currentChange;

        ASSERT_TRUE(query(createCyrusSuhas, emptyCallback));
    }

    {
        newChange(), change2 = _currentChange;

        ASSERT_TRUE(query(deleteSuhas, emptyCallback));
    }

    submitChange(change1);

    setChange(change2);

    auto res = query("CHANGE SUBMIT", emptyCallback);

    EXPECT_FALSE(res);
    ASSERT_TRUE(res.hasErrorMessage());
    EXPECT_EQ(expectedError, res.getError());
}

/*
Testing the following flow:

1. Change1 is created and creates two nodes, and an edge between them
2. Submits
3. Change2 is created deletes the source node
4. Submits
5. Change3 is created deletes the target node
6. Submits

This should be accepted
*/
TEST_F(ChangeQueriesTest, noConflictOnDeletedEdge) {
    setWorkingGraph("default");

    {
        newChange();
        std::string_view createQuery = "CREATE (n:Source)-[e:NEWEDGE]->(m:Target)";
        query(createQuery, emptyCallback);
        submitCurrentChange();
    }

    {
        newChange();
        // This should also delete the edge "e" created above
        std::string_view deleteSource = "MATCH (n:Source) DELETE n";
        query(deleteSource, emptyCallback);
        submitCurrentChange();
    }

    {
        newChange();
        // This would result in deleting edge "e", but it was already deleted: no conflict
        std::string_view deleteTarget = "MATCH (n:Target) DELETE n";
        query(deleteTarget, emptyCallback);
        submitCurrentChange();
    }
}

/*
Testing the following flow:

1. Change1 is created and creates two nodes, and an edge between them
2. Submits
3. Change 2 is created
4. Change 3 is created
5. Change 2 deletes the source node
6. Change 3 deletes the target node
7. Change 2 submits -> accepted
8 Change 3 submits -> rejected (write conflict on the edge)
*/
TEST_F(ChangeQueriesTest, conflictOnDeletedEdge) {
    setWorkingGraph("default");

    {
        newChange();
        std::string_view createQuery = "CREATE (n:Source)-[e:NEWEDGE]->(m:Target)";
        ASSERT_TRUE(query(createQuery, emptyCallback));
        submitCurrentChange();
    }

    ChangeID change2, change3;

    newChange(), change2 = _currentChange;
    newChange(), change3 = _currentChange;

    {
        setChange(change2);
        ASSERT_TRUE(query("MATCH (n:Source) DELETE n", emptyCallback));
    }

    {
        setChange(change3);
        ASSERT_TRUE(query("MATCH (n:Target) DELETE n", emptyCallback));
    }

    submitChange(change3); // Should succeed

    setChange(change2);
    auto res = query("CHANGE SUBMIT", emptyCallback);

    EXPECT_FALSE(res);
    ASSERT_TRUE(res.hasErrorMessage());
    std::string_view expectedError =
        "This change attempted to delete Edge 0 (which is now Edge "
        "0 on main) which has been modified on main.";
    EXPECT_EQ(expectedError, res.getError());
}

TEST_F(ChangeQueriesTest, resolvedOutEdgesDeleteConflict) {
    ChangeID change0, change1;

    newChange(), change0 = _currentChange;
    newChange(), change1 = _currentChange;

    { // Try delete Luc
        setChange(change0);
        ASSERT_TRUE(query(R"(MATCH (n) WHERE n.name = "Luc" DELETE n)", emptyCallback));
    }

    { // But create an edge between Luc and Cyrus
        setChange(change1);
        std::string_view createLucCyrus =
            R"(MATCH (l), (c) WHERE l.name = "Luc" AND c.name = "Cyrus" CREATE (l)-[:WORKS_WITH]->(c))";

        ASSERT_TRUE(query(createLucCyrus, emptyCallback));
        submitCurrentChange();
    }

    { // But the actually just delete that edge
        newChange(); // Change 3
        std::string_view deleteLucCyrus =
            R"(MATCH (l)-[e]->(c) WHERE l.name = "Luc" AND c.name = "Cyrus" DELETE e)";
        ASSERT_TRUE(query(deleteLucCyrus, emptyCallback));
        submitCurrentChange();
    }

    { // So Change 0 can delete Luc since the graph is the same as when it branched
        setChange(change0);
        submitCurrentChange(); // This should succeed
    }
}

TEST_F(ChangeQueriesTest, resolvedOutEdgesDeleteConflictWithCommit) {
    ChangeID change0, change1;

    newChange(), change0 = _currentChange;
    newChange(), change1 = _currentChange;

    { // Try delete Luc
        setChange(change0);
        ASSERT_TRUE(query(R"(MATCH (n) WHERE n.name = "Luc" DELETE n)", emptyCallback));
        ASSERT_TRUE(query("COMMIT", emptyCallback));
    }

    { // But create an edge between Luc and Cyrus
        setChange(change1);
        std::string_view createLucCyrus =
            R"(MATCH (l), (c) WHERE l.name = "Luc" AND c.name = "Cyrus" CREATE (l)-[:WORKS_WITH]->(c))";

        ASSERT_TRUE(query(createLucCyrus, emptyCallback));
        submitCurrentChange();
    }

    { // But the actually just delete that edge
        newChange(); // Change 3
        std::string_view deleteLucCyrus =
            R"(MATCH (l)-[e]->(c) WHERE l.name = "Luc" AND c.name = "Cyrus" DELETE e)";
        ASSERT_TRUE(query(deleteLucCyrus, emptyCallback));
        submitCurrentChange();
    }

    { // So Change 0 can delete Luc since the graph is the same as when it branched
        setChange(change0);
        submitCurrentChange(); // This should succeed
    }
}

TEST_F(ChangeQueriesTest, rebasedTombstones) {
    const PropertyTypeID nameID = 0;

    using String = types::String::Primitive;
    using Rows = LineContainer<String>;

    Rows expected;
    {
        for (auto&& name : read().scanNodeProperties<types::String>(nameID)) {
            expected.add({name});
        }
        expected.add({"Eleanor"});
        expected.add({"Cats"});
        expected.add({"Music"});
        expected.add({"Dogs"});
    }

    ChangeID change0, change1;

    newChange(), change0 = _currentChange;
    newChange(), change1 = _currentChange;

    { // Create changes unknown to change1
        setChange(change0);

        std::string_view createEleanor = R"(CREATE (e:Person{name:"Eleanor"}))";
        std::string_view createCats = R"(CREATE (c:Interest{name:"Cats"}))";

        ASSERT_TRUE(query(createEleanor, emptyCallback));
        ASSERT_TRUE(query(createCats, emptyCallback));

        submitCurrentChange();
    }

    { // Create new nodes as well, but delete some of them
        setChange(change1);

        std::string_view createManchester = R"(CREATE (n:Interest{name:"Manchester"}))";
        std::string_view createMusic = R"(CREATE (n:Interest{name:"Music"}))";
        std::string_view createDogs = R"(CREATE (n:Interest{name:"Dogs"}))";

        for (auto&& q : {createManchester, createMusic, createDogs}) {
            ASSERT_TRUE(query(q, emptyCallback));
        }

        ASSERT_TRUE(query("COMMIT", emptyCallback));

        std::string_view deleteManchester =
            R"(MATCH (m) WHERE m.name = "Manchester" DELETE m)";

        ASSERT_TRUE(query(deleteManchester, emptyCallback));

        // Change1's new nodes will need to be rebased, but Manchester should be deleted
        submitCurrentChange();
    }

    Rows actual;
    {
        std::string_view getNames = "MATCH (n) RETURN n.name";

        auto res = query(getNames, [&actual](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(1, df->size());

            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(names);

            const size_t numRows = names->size();
            for (size_t row {0}; row < numRows; row++) {
                actual.add({*names->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(ChangeQueriesTest, rebasedTombstonesWithCommit) {
    const PropertyTypeID nameID = 0;

    using String = types::String::Primitive;
    using Rows = LineContainer<String>;

    Rows expected;
    {
        for (auto&& name : read().scanNodeProperties<types::String>(nameID)) {
            expected.add({name});
        }
        expected.add({"Eleanor"});
        expected.add({"Cats"});
        expected.add({"Music"});
        expected.add({"Dogs"});
    }

    ChangeID change0, change1;

    newChange(), change0 = _currentChange;
    newChange(), change1 = _currentChange;

    { // Create changes unknown to change1
        setChange(change0);

        std::string_view createEleanor = R"(CREATE (e:Person{name:"Eleanor"}))";
        std::string_view createCats = R"(CREATE (c:Interest{name:"Cats"}))";

        ASSERT_TRUE(query(createEleanor, emptyCallback));
        ASSERT_TRUE(query(createCats, emptyCallback));

        submitCurrentChange();
    }

    { // Create new nodes as well, but delete some of them
        setChange(change1);

        std::string_view createManchester = R"(CREATE (n:Interest{name:"Manchester"}))";
        std::string_view createMusic = R"(CREATE (n:Interest{name:"Music"}))";
        std::string_view createDogs = R"(CREATE (n:Interest{name:"Dogs"}))";

        ASSERT_TRUE(query(createManchester, emptyCallback));
        ASSERT_TRUE(query("COMMIT", emptyCallback));
        ASSERT_TRUE(query(createMusic, emptyCallback));
        ASSERT_TRUE(query(createDogs, emptyCallback));
        ASSERT_TRUE(query("COMMIT", emptyCallback));

        std::string_view deleteManchester =
            R"(MATCH (m) WHERE m.name = "Manchester" DELETE m)";

        ASSERT_TRUE(query(deleteManchester, emptyCallback));

        // Change1's new nodes will need to be rebased, but Manchester should be deleted
        submitCurrentChange();
    }

    Rows actual;
    {
        std::string_view getNames = "MATCH (n) RETURN n.name";

        auto res = query(getNames, [&actual](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(1, df->size());

            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(names);

            const size_t numRows = names->size();
            for (size_t row {0}; row < numRows; row++) {
                actual.add({*names->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}
