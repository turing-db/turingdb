#include <gtest/gtest.h>

#include <initializer_list>
#include <string_view>
#include <cstdint>
#include <range/v3/view/zip.hpp>

#include "TuringDB.h"
#include "QueryConfig.h"
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

namespace rg = ranges;
namespace rv = rg::views;

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
    QueryConfig _queryConfig;
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

    auto query(std::string_view query, auto callback, ChangeID change) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), change);
        return _db->query(query, state);
    }

    auto query(std::string_view q, auto callback) {
        return query(q, callback, _currentChange);
    }

    void submitCurrentChange() {
        auto res = query("CHANGE SUBMIT", emptyCallback);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    void submitChange(ChangeID chid) {
        auto res = query("CHANGE SUBMIT", emptyCallback, chid);
        ASSERT_TRUE(res) << res.getError();
        _currentChange = ChangeID::head();
    }

    void setChange(ChangeID chid) {
        _currentChange = chid;
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

    constexpr static auto dump = [](const Dataframe* df) {
        std::ostringstream out;
        df->dump(out);
        return out.str();
    };

    // Helper to check the output of a db.propertyTypes()
    bool ensureProperties(std::initializer_list<std::string_view> props) {
        bool allPropsFound = true;

        auto res = query(GET_PROPERTIES_QUERY, [&](const Dataframe* df) {
            ASSERT_TRUE(df);

            NamedColumn* propCol = findColumn(df, "property");

            ASSERT_TRUE(propCol);

            auto* castedProps = propCol->as<ColumnVector<std::string_view>>();
            ASSERT_TRUE(castedProps);

            for (const auto& desired : props) {
                bool found = false;
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

                for (size_t row = 0; row < ns->size(); row++) {
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

                for (size_t row = 0; row < df->getLogicalRowCount(); row++) {
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

                for (size_t row = 0; row < df->getLogicalRowCount(); row++) {
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
            for (size_t row = 0; row < numRows; row++) {
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
            for (size_t row = 0; row < numRows; row++) {
                actual.add({*names->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(ChangeQueriesTest, concurrentWritesSmall) {
    setWorkingGraph("default");
    const size_t change0Nodes = 1;
    const size_t change1Nodes = 3;

    ChangeID change0, change1;

    newChange(), change0 = _currentChange;
    newChange(), change1 = _currentChange;

    {
        setChange(change0);
        ASSERT_TRUE(query("CREATE (n:NODE1)", emptyCallback));
    }

    {
        setChange(change1);
        ASSERT_TRUE(query("CREATE (n:NODE2)", emptyCallback));
        ASSERT_TRUE(query("COMMIT", emptyCallback));
        ASSERT_TRUE(query("CREATE (:NODE3)-[:NEWEDGE1]->(:NODE4)", emptyCallback));
    }

    submitChange(change0);
    submitChange(change1);

    {
        using Rows = LineContainer<NodeID>;

        Rows expected;
        {
            for (size_t node = 0; node < change0Nodes + change1Nodes; node++) {
                expected.add({node});
            }
        }

        Rows actual;
        {
            auto res = query("MATCH (n) RETURN n", [&actual](const Dataframe* df) {
                auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);

                for (NodeID n : *ns) {
                    actual.add({n});
                }
            });
            ASSERT_TRUE(res);
        }
        EXPECT_TRUE(expected.equals(actual));
    }

    {
        using Rows = LineContainer<NodeID, EdgeID, NodeID>;

        Rows expected;
        {
            expected.add({2, 0, 3});
        }

        Rows actual;
        {
            auto res = query("MATCH (n)-[e]->(m) RETURN n,e,m", [&actual](const Dataframe* df) {
                auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
                auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
                auto* ms = findColumn(df, "m")->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                ASSERT_TRUE(es);
                ASSERT_TRUE(ms);

                const size_t numRows = ns->size();
                for (size_t row = 0; row < numRows; row++) {
                    actual.add({ns->at(row), es->at(row), ms->at(row)});
                }
            });
            ASSERT_TRUE(res);
        }
        EXPECT_TRUE(expected.equals(actual));
    }
}

TEST_F(ChangeQueriesTest, concurrentWritesLarge) {
    setWorkingGraph("default");

    const size_t change0SingleNodes = 9;
    const size_t change0EdgePairs = 6;

    const size_t change1SingleNodes = 6;
    const size_t change1EdgePairs = 4;

    ChangeID change0, change1;

    {
        newChange(), change0 = _currentChange;

        for (size_t i = 0; i < change0SingleNodes; i++) {
            ASSERT_TRUE(query("CREATE (:NODE)", emptyCallback));
        }

        ASSERT_TRUE(query("COMMIT", emptyCallback));

        for (size_t i = 0; i < change0EdgePairs; i++) {
            ASSERT_TRUE(query("CREATE (:NODE)-[:EDGE]->(:NODE)", emptyCallback));
        }
    }

    {
        newChange(), change1 = _currentChange;

        for (size_t i = 0; i < change1SingleNodes; i++) {
            ASSERT_TRUE(query("CREATE (:NODE)", emptyCallback));
        }

        ASSERT_TRUE(query("COMMIT", emptyCallback));

        for (size_t i = 0; i < change1EdgePairs; i++) {
            ASSERT_TRUE(query("CREATE (:NODE)-[:EDGE]->(:NODE)", emptyCallback));
        }
    }

    submitChange(change0);
    submitChange(change1);

    const size_t expectedNodes = change0SingleNodes + change1SingleNodes
                               + (2 * change0EdgePairs) + (2 * change1EdgePairs);

    {
        using Rows = LineContainer<NodeID>;

        Rows expected;
        for (NodeID n(0); n < expectedNodes; n++) {
            expected.add({n});
        }

        Rows actual;
        {
            auto res = query("MATCH (n) RETURN n", [&actual](const Dataframe* df) {
                auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                for (NodeID n : *ns) {
                    actual.add({n});
                }
            });
            ASSERT_TRUE(res);
        }
        EXPECT_TRUE(expected.equals(actual));
    }

    {
        using Rows = LineContainer<NodeID, EdgeID, NodeID>;

        Rows expected;
        {
            EdgeID e(0);
            NodeID n(change0SingleNodes);
            for (size_t i = 0; i < change0EdgePairs; i++) {
                NodeID src = n;
                NodeID tgt = n + 1;
                expected.add({src, e++, tgt});
                n = tgt + 1;
            }
            n += change1SingleNodes;
            for (size_t i = change1SingleNodes; i < change1SingleNodes + change1EdgePairs;
                 i++) {
                NodeID src = n;
                NodeID tgt = n + 1;
                expected.add({src, e++, tgt});
                n = tgt + 1;
            }
        }

        Rows actual;
        {
            auto res = query("MATCH (n)-[e]->(m) RETURN n,e,m", [&actual](const Dataframe* df) {
                auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
                auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
                auto* ms = findColumn(df, "m")->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                ASSERT_TRUE(es);
                ASSERT_TRUE(ms);
                for (size_t i = 0; i < ns->size(); i++) {
                    actual.add({ns->at(i), es->at(i), ms->at(i)});
                }
            });
            ASSERT_TRUE(res);
        }
        EXPECT_TRUE(expected.equals(actual));
    }
}

TEST_F(ChangeQueriesTest, historyWithDeletions) {
    using HistoryCountColumn = ColumnVector<uint64_t>;

    setWorkingGraph("default");

    const size_t numNodes = 4;

    {
        newChange();
        for (size_t i = 0; i < numNodes; i++) {
            ASSERT_TRUE(query("CREATE (n:Node)", emptyCallback));
        }
        submitCurrentChange();
    }

    {
        newChange();
        ASSERT_TRUE(query("MATCH (n) DELETE n", emptyCallback));
        submitCurrentChange();
    }

    auto res = query("CALL db.history()", [](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* nodes = findColumn(df, "nodeCount")->as<HistoryCountColumn>();
        auto* edges = findColumn(df, "edgeCount")->as<HistoryCountColumn>();
        auto* parts = findColumn(df, "partCount")->as<HistoryCountColumn>();
        ASSERT_TRUE(nodes && edges && parts);

        {
            HistoryCountColumn expectedNodeCounts = {0, 4, 0};
            ASSERT_EQ(expectedNodeCounts.size(), nodes->size());
            for (auto [exp, act] : rv::zip(expectedNodeCounts, *nodes)) {
                EXPECT_EQ(exp, act);
            }
        }

        {
            HistoryCountColumn expectedEdgeCounts = {0, 0, 0};
            ASSERT_EQ(expectedEdgeCounts.size(), edges->size());
            for (auto [exp, act] : rv::zip(expectedEdgeCounts, *edges)) {
                EXPECT_EQ(exp, act);
            }
        }

        {
            HistoryCountColumn expectedPartCounts = {0, 1, 0};
            ASSERT_EQ(expectedPartCounts.size(), parts->size());
            for (auto [exp, act] : rv::zip(expectedPartCounts, *parts)) {
                EXPECT_EQ(exp, act);
            }
        }
    });
    ASSERT_TRUE(res);
}

TEST_F(ChangeQueriesTest, labelSetsSameCommitRebase) {
    setWorkingGraph("default");
    ChangeID changeA;
    ChangeID changeB;
    {
        newChange(), changeA = _currentChange;
        constexpr std::string_view CREATE_QUERY =
            R"(CREATE (n1:Person{name:"Head"})-[:FOLLOWS]->(n2:Person{name:"Mid"})-[:FOLLOWS]->(n3:Person{name:"Tail"}))";
        {
            const auto res = query(CREATE_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
    }
    {
        newChange(), changeB = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"Lone"}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
    }

    submitChange(changeB);
    submitChange(changeA);

    {
        constexpr std::string_view CHAIN_QUERY = R"(MATCH (n1:Person)-[:FOLLOWS]->(n2:Person)-[:FOLLOWS]->(n3:Person) RETURN n1.name, n2.name, n3.name)";
        const auto res2 = query(CHAIN_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(3, df->size());
            const auto* n1Names = findColumn(df, "n1.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* n2Names = findColumn(df, "n2.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* n3Names = findColumn(df, "n3.name")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(n1Names && n2Names && n3Names);

            ASSERT_FALSE(n1Names->empty() || n2Names->empty() || n3Names->empty());

            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Head" }), n1Names->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Mid" }), n2Names->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Tail" }), n3Names->getRaw()) << dump(df);
        });
        ASSERT_TRUE(res2) << res2.getError();
    }
}

TEST_F(ChangeQueriesTest, setNodeConflict) {
    ChangeID change1;
    ChangeID change2;
    {
        newChange(), change1 = _currentChange;
        constexpr std::string_view SET_QUERY = R"(MATCH (n) SET n.age = 100)";
        ASSERT_TRUE(query(SET_QUERY, emptyCallback));
    }
    {
        newChange(), change2 = _currentChange;
        constexpr std::string_view SET_QUERY = R"(MATCH (n) SET n.age = 1000)";
        ASSERT_TRUE(query(SET_QUERY, emptyCallback));
    }

    submitChange(change2);
    setChange(change1);

    auto res = query("CHANGE SUBMIT", emptyCallback);
    EXPECT_FALSE(res);

    ASSERT_TRUE(res.hasErrorMessage());

    const std::string_view err = res.getError();
    const std::string_view expectedErr =
        "This change attempted to update Node 0 (which is now "
        "Node 0 on main) which has been modified on main.";

    EXPECT_EQ(expectedErr, err);
}

TEST_F(ChangeQueriesTest, setEdgeConflict) {
    ChangeID change1;
    ChangeID change2;
    {
        newChange(), change1 = _currentChange;
        constexpr std::string_view SET_QUERY = R"(MATCH ()-[e]->() SET e.duration = 100)";
        ASSERT_TRUE(query(SET_QUERY, emptyCallback));
    }
    {
        newChange(), change2 = _currentChange;
        constexpr std::string_view SET_QUERY = R"(MATCH ()-[e]->() SET e.duration = 1000)";
        ASSERT_TRUE(query(SET_QUERY, emptyCallback));
    }

    submitChange(change2);
    setChange(change1);

    auto res = query("CHANGE SUBMIT", emptyCallback);
    EXPECT_FALSE(res);

    ASSERT_TRUE(res.hasErrorMessage());

    const std::string_view err = res.getError();
    const std::string_view expectedErr =
        "This change attempted to update Edge 0 (which is now "
        "Edge 0 on main) which has been modified on main.";

    EXPECT_EQ(expectedErr, err);
}

TEST_F(ChangeQueriesTest, setNodeConflictDifferentProps) {
    ChangeID change1;
    ChangeID change2;
    {
        newChange(), change1 = _currentChange;
        constexpr std::string_view SET_QUERY = R"(MATCH (n) SET n.name = "new")";
        ASSERT_TRUE(query(SET_QUERY, emptyCallback));
    }
    {
        newChange(), change2 = _currentChange;
        constexpr std::string_view SET_QUERY = R"(MATCH (n) SET n.age = 1000)";
        ASSERT_TRUE(query(SET_QUERY, emptyCallback));
    }

    submitChange(change2);
    setChange(change1);

    auto res = query("CHANGE SUBMIT", emptyCallback);
    EXPECT_FALSE(res);

    ASSERT_TRUE(res.hasErrorMessage());

    const std::string_view err = res.getError();
    const std::string_view expectedErr =
        "This change attempted to update Node 0 (which is now "
        "Node 0 on main) which has been modified on main.";

    EXPECT_EQ(expectedErr, err);
}

TEST_F(ChangeQueriesTest, setEdgeConflictDifferentProps) {
    ChangeID change1;
    ChangeID change2;
    {
        newChange(), change1 = _currentChange;
        constexpr std::string_view SET_QUERY = R"(MATCH ()-[e]->() SET e.name = "new")";
        ASSERT_TRUE(query(SET_QUERY, emptyCallback));
    }
    {
        newChange(), change2 = _currentChange;
        constexpr std::string_view SET_QUERY = R"(MATCH ()-[e]->() SET e.duration = 1000)";
        ASSERT_TRUE(query(SET_QUERY, emptyCallback));
    }

    submitChange(change2);
    setChange(change1);

    auto res = query("CHANGE SUBMIT", emptyCallback);
    EXPECT_FALSE(res);

    ASSERT_TRUE(res.hasErrorMessage());

    const std::string_view err = res.getError();
    const std::string_view expectedErr =
        "This change attempted to update Edge 0 (which is now "
        "Edge 0 on main) which has been modified on main.";

    EXPECT_EQ(expectedErr, err);
}

// Two changes (A and B) both create nodes locally, Change A modifies that node with a
// SET. Change B submits first. Change A submits second. A's modifications should apply to
// the node it created, not the node B created.
TEST_F(ChangeQueriesTest, setThenRebase) {
    setWorkingGraph("default");

    ChangeID change1;
    ChangeID change2;

    {
        newChange(), change1 = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Interest{name:"Dogs"}))";
        constexpr std::string_view SET_QUERY = R"(MATCH (n) WHERE n.name = "Dogs" SET n.name = "Cats")";

        {
            const auto res = query(CREATE_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }

        {
            const auto res = query(SET_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
    }

    {
        newChange(), change2 = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Interest{name:"Music"}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
        submitCurrentChange();
    }

    setChange(change1);
    submitCurrentChange();

    {
        constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.name)";
        const auto res = query(MATCH_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(2, df->size());

            const auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns && names);

            ASSERT_FALSE(ns->empty());
            ASSERT_FALSE(names->empty());

            EXPECT_EQ((std::vector<NodeID>{0, 1}), ns->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>> {"Music", "Cats"}),
                      names->getRaw())
                << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(ChangeQueriesTest, setIntPropertyThenRebase) {
    setWorkingGraph("default");
    ChangeID changeA;
    ChangeID changeB;
    {
        newChange(), changeA = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{age:1}))";
        constexpr std::string_view SET_QUERY = R"(MATCH (n:Person) WHERE n.age = 1 SET n.age = 2)";
        {
            const auto res = query(CREATE_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
        {
            const auto res = query(SET_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
    }
    {
        newChange(), changeB = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{age:99}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
        submitCurrentChange();
    }
    setChange(changeA);
    submitCurrentChange();
    {
        constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.age)";
        const auto res = query(MATCH_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(2, df->size());
            const auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            const auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(ns && ages);
            ASSERT_FALSE(ns->empty());
            ASSERT_FALSE(ages->empty());
            EXPECT_EQ((std::vector<NodeID> {0, 1}), ns->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Int64::Primitive>>{99, 2}), ages->getRaw()) << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(ChangeQueriesTest, threeChangesSetThenRebaseLastSubmit) {
    setWorkingGraph("default");
    ChangeID changeA;
    ChangeID changeB;
    ChangeID changeC;
    {
        newChange(), changeA = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"Alpha"}))";
        constexpr std::string_view SET_QUERY    = R"(MATCH (n:Person) WHERE n.name = "Alpha" SET n.name = "Beta")";
        {
            const auto res = query(CREATE_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
        {
            const auto res = query(SET_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
    }
    {
        newChange(), changeB = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"Gamma"}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
    }
    {
        newChange(), changeC = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"Delta"}))";
        constexpr std::string_view SET_QUERY = R"(MATCH (n) WHERE n.name = "Delta" SET n.name = "Mu")";
        {
            const auto res = query(CREATE_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
        {
            const auto res = query(SET_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
    }
    submitChange(changeB);
    submitChange(changeC);
    submitChange(changeA);
    {
        constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.name)";
        const auto res = query(MATCH_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(2, df->size());
            const auto* ns    = findColumn(df, "n")->as<ColumnNodeIDs>();
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns && names);
            ASSERT_FALSE(ns->empty());
            ASSERT_FALSE(names->empty());
            EXPECT_EQ((std::vector<NodeID>{ 0, 1, 2}), ns->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Gamma", "Mu", "Beta"}), names->getRaw()) << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(ChangeQueriesTest, setPropertyTwiceThenRebase) {
    setWorkingGraph("default");
    ChangeID changeA;
    ChangeID changeB;
    {
        newChange(), changeA = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"V1"}))";
        constexpr std::string_view SET_QUERY_1  = R"(MATCH (n:Person) WHERE n.name = "V1" SET n.name = "V2")";
        constexpr std::string_view SET_QUERY_2  = R"(MATCH (n:Person) WHERE n.name = "V2" SET n.name = "V3")";
        {
            const auto res = query(CREATE_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
        {
            const auto res = query(SET_QUERY_1, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
        {
            const auto res = query(SET_QUERY_2, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
    }
    {
        newChange(), changeB = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"Other"}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
    }
    submitChange(changeB);
    submitChange(changeA);
    {
        constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.name)";
        const auto res = query(MATCH_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(2, df->size());
            const auto* ns    = findColumn(df, "n")->as<ColumnNodeIDs>();
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns && names);
            ASSERT_FALSE(ns->empty());
            ASSERT_FALSE(names->empty());
            EXPECT_EQ((std::vector<NodeID> {0, 1}), ns->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Other", "V3" }), names->getRaw()) << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(ChangeQueriesTest, setTwoCreatedNodesThenRebase) {
    setWorkingGraph("default");
    ChangeID changeA;
    ChangeID changeB;
    {
        newChange(), changeA = _currentChange;
        constexpr std::string_view CREATE_1  = R"(CREATE (n:Person{name:"First"}))";
        constexpr std::string_view CREATE_2  = R"(CREATE (n:Person{name:"First"}))";
        constexpr std::string_view SET_QUERY = R"(MATCH (n:Person) WHERE n.name = "First" OR n.name = "Second" SET n.name = "Updated")";
        {
            const auto res = query(CREATE_1, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
        {
            const auto res = query(CREATE_2, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
        {
            const auto res = query(SET_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
    }
    {
        newChange(), changeB = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"Third"}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
    }

    submitChange(changeB);
    submitChange(changeA);

    {
        constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.name)";
        const auto res = query(MATCH_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(2, df->size());
            const auto* ns    = findColumn(df, "n")->as<ColumnNodeIDs>();
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns && names);
            ASSERT_FALSE(ns->empty());
            ASSERT_FALSE(names->empty());
            EXPECT_EQ((std::vector<NodeID>{ 0, 1, 2 }), ns->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Third", "Updated", "Updated" }), names->getRaw()) << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(ChangeQueriesTest, threeChangesAllPropertyTypesThenRebase) {
    setWorkingGraph("default");
    ChangeID changeA;
    ChangeID changeB;
    ChangeID changeC;
    {
        newChange(), changeA = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"Alice", age:30, hasPhD:true}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
        ASSERT_TRUE(query("commit", emptyCallback));
        constexpr std::string_view SET_QUERY = R"(MATCH (n:Person) WHERE n.name = "Alice" SET n.age = 31)";
        const auto res2 = query(SET_QUERY, emptyCallback);
        ASSERT_TRUE(res2) << res2.getError();
    }
    {
        newChange(), changeB = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"Bob", age:25, hasPhD:false}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
        ASSERT_TRUE(query("commit", emptyCallback));
        constexpr std::string_view SET_QUERY = R"(MATCH (n:Person) WHERE n.name = "Bob" SET n.hasPhD = true)";
        const auto res2 = query(SET_QUERY, emptyCallback);
        ASSERT_TRUE(res2) << res2.getError();
    }
    {
        newChange(), changeC = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"Carol"}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
    }

    submitChange(changeC);
    submitChange(changeA);
    submitChange(changeB);

    {
        constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.name, n.age, n.hasPhD)";
        const auto res = query(MATCH_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(4, df->size());
            const auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* hasPhDs = findColumn(df, "n.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(ns && names && ages && hasPhDs);
            ASSERT_FALSE(ns->empty());
            EXPECT_EQ((std::vector<NodeID>{0, 1, 2}), ns->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Carol", "Alice", "Bob" }), names->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Int64::Primitive>>{ {}, 31, 25}), ages->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Bool::Primitive>>{ {}, true, true}), hasPhDs->getRaw()) << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(ChangeQueriesTest, setNodeAndEdgePropertiesThenRebase) {
    setWorkingGraph("default");
    ChangeID changeA;
    ChangeID changeB;

    {
        newChange(), changeA = _currentChange;
        constexpr std::string_view CREATE_QUERY =
            R"(CREATE (n:Person{name:"Eve"})-[e:KNOWS{age:5}]->(m:Person{name:"Frank"}))";
        constexpr std::string_view SET_QUERY =
            R"(MATCH (n:Person)-[e:KNOWS]->(m:Person) WHERE n.name = "Eve" SET e.age = 10, n.hasPhD = true)";
        {
            const auto res = query(CREATE_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
        {
            const auto res = query(SET_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
    }
    {
        newChange(), changeB = _currentChange;
        constexpr std::string_view CREATE_QUERY =
            R"(CREATE (n:Person{name:"Grace"})-[e:KNOWS{age:99}]->(m:Person{name:"Henry"}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
    }

    submitChange(changeB);
    submitChange(changeA);

    {
        constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e:KNOWS]->(m) RETURN n, n.name, n.hasPhD, m.name, e, e.age)";
        const auto res = query(MATCH_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(6, df->size());

            const auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            const auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            const auto* nNames = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* hasPhDs = findColumn(df, "n.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            const auto* mNames = findColumn(df, "m.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* eages = findColumn(df, "e.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(ns && nNames && hasPhDs && mNames && eages);
            ASSERT_FALSE(ns->empty());

            EXPECT_EQ((std::vector<NodeID>{ 0, 2 }), ns->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<EdgeID>{ 0, 1 }), es->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Grace", "Eve" }), nNames->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Bool::Primitive>>{ {}, true }), hasPhDs->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Henry", "Frank" }), mNames->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Int64::Primitive>>{ 99, 10 }), eages->getRaw()) << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(ChangeQueriesTest, fourChangesReverseSubmitOrder) {
    setWorkingGraph("default");
    ChangeID changeA;
    ChangeID changeB;
    ChangeID changeC;
    ChangeID changeD;
    {
        newChange(), changeA = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"A-Node"}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
        ASSERT_TRUE(query("commit", emptyCallback));
        constexpr std::string_view SET_QUERY = R"(MATCH (n:Person) WHERE n.name = "A-Node" SET n.age = 1)";
        const auto res2 = query(SET_QUERY, emptyCallback);
        ASSERT_TRUE(res2) << res2.getError();
    }
    {
        newChange(), changeB = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"B-Node"}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
        ASSERT_TRUE(query("commit", emptyCallback));
        constexpr std::string_view SET_QUERY = R"(MATCH (n:Person) WHERE n.name = "B-Node" SET n.age = 2)";
        const auto res2 = query(SET_QUERY, emptyCallback);
        ASSERT_TRUE(res2) << res2.getError();
    }
    {
        newChange(), changeC = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"C-Node"}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
        ASSERT_TRUE(query("commit", emptyCallback));
        constexpr std::string_view SET_QUERY = R"(MATCH (n:Person) WHERE n.name = "C-Node" SET n.age = 3)";
        const auto res2 = query(SET_QUERY, emptyCallback);
        ASSERT_TRUE(res2) << res2.getError();
    }
    {
        newChange(), changeD = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"D-Node"}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
        ASSERT_TRUE(query("commit", emptyCallback));
        constexpr std::string_view SET_QUERY = R"(MATCH (n:Person) WHERE n.name = "D-Node" SET n.age = 4)";
        const auto res2 = query(SET_QUERY, emptyCallback);
        ASSERT_TRUE(res2) << res2.getError();
    }
    submitChange(changeD);
    submitChange(changeC);
    submitChange(changeB);
    submitChange(changeA);
    {
        constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.name, n.age)";
        const auto res = query(MATCH_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(3, df->size());
            const auto* ns    = findColumn(df, "n")->as<ColumnNodeIDs>();
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ages  = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(ns && names && ages);
            ASSERT_FALSE(ns->empty());
            EXPECT_EQ((std::vector<NodeID> {0, 1, 2, 3}), ns->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{"D-Node", "C-Node", "B-Node", "A-Node"}), names->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Int64::Primitive>>{4, 3, 2, 1}), ages->getRaw()) << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(ChangeQueriesTest, setChainedNodesThenRebase) {
    setWorkingGraph("default");
    ChangeID changeA;
    ChangeID changeB;
    {
        newChange(), changeA = _currentChange;
        constexpr std::string_view CREATE_QUERY =
            R"(CREATE (n1:Person{name:"Head"})-[:FOLLOWS]->(n2:Person{name:"Mid"})-[:FOLLOWS]->(n3:Person{name:"Tail"}))";
        constexpr std::string_view SET_QUERY =
            R"(MATCH (n:Person) WHERE n.name = "Mid" SET n.hasPhD = true)";
        {
            const auto res = query(CREATE_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
        {
            const auto res = query(SET_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
    }
    {
        newChange(), changeB = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"Lone", hasPhD:true}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
    }

    submitChange(changeB);
    submitChange(changeA);

    {
        // Verify all four nodes
        constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.name, n.hasPhD)";
        const auto res = query(MATCH_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(3, df->size());
            const auto* ns      = findColumn(df, "n")->as<ColumnNodeIDs>();
            const auto* names   = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* hasPhDs = findColumn(df, "n.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(ns && names && hasPhDs);
            ASSERT_FALSE(ns->empty());
            EXPECT_EQ((std::vector<NodeID> {0, 1, 2, 3}), ns->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Lone", "Head", "Mid", "Tail" }), names->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Bool::Primitive>>{ true, {}, {}, {} }), hasPhDs->getRaw()) << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
        constexpr std::string_view CHAIN_QUERY = R"(MATCH (n1:Person)-[:FOLLOWS]->(n2:Person)-[:FOLLOWS]->(n3:Person) RETURN n1.name, n2.name, n3.name)";
        const auto res2 = query(CHAIN_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(3, df->size());
            const auto* n1Names = findColumn(df, "n1.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* n2Names = findColumn(df, "n2.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* n3Names = findColumn(df, "n3.name")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(n1Names && n2Names && n3Names);

            ASSERT_FALSE(n1Names->empty() || n2Names->empty() || n3Names->empty());

            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Head" }), n1Names->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Mid" }), n2Names->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Tail" }), n3Names->getRaw()) << dump(df);
        });
        ASSERT_TRUE(res2) << res2.getError();
    }
}

TEST_F(ChangeQueriesTest, setAllPropertiesTwiceThenRebase) {
    setWorkingGraph("default");
    ChangeID changeA;
    ChangeID changeB;
    ChangeID changeC;
    {
        newChange(), changeA = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"AFirst", age:1, hasPhD:false}))";
        constexpr std::string_view SET_QUERY = R"(MATCH (n:Person) WHERE n.name = "AFirst" SET n.name = "ASecond", n.age = 2, n.hasPhD = true)";
        {
            const auto res = query(CREATE_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
        {
            const auto res = query(SET_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
    }
    {
        newChange(), changeB = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"BNode", age:50, hasPhD:false}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
    }
    {
        newChange(), changeC = _currentChange;
        constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person{name:"CNode"}))";
        const auto res = query(CREATE_QUERY, emptyCallback);
        ASSERT_TRUE(res) << res.getError();
    }
    submitChange(changeB);
    submitChange(changeC);
    submitChange(changeA);
    {
        constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.name, n.age, n.hasPhD)";
        const auto res = query(MATCH_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(4, df->size());
            const auto* ns      = findColumn(df, "n")->as<ColumnNodeIDs>();
            const auto* names   = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ages    = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* hasPhDs = findColumn(df, "n.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(ns && names && ages && hasPhDs);
            ASSERT_FALSE(ns->empty());
            EXPECT_EQ((std::vector<NodeID> {0, 1, 2}), ns->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "BNode", "CNode", "ASecond" }), names->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Int64::Primitive>>{50, {}, 2}), ages->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Bool::Primitive>>{ false, {}, true }), hasPhDs->getRaw()) << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(ChangeQueriesTest, threeChangesSubgraphsComplexRebase) {
    setWorkingGraph("default");
    ChangeID changeA;
    ChangeID changeB;
    ChangeID changeC;
    {
        newChange(), changeA = _currentChange;
        constexpr std::string_view CREATE_QUERY =
            R"(CREATE (n:Person{name:"Ava", hasPhD:true})-[e:KNOWS{age:1}]->(m:Person{name:"Ben"}))";
        constexpr std::string_view SET_QUERY =
            R"(MATCH (n:Person)-[e:KNOWS]->(m:Person) WHERE n.name = "Ava" SET n.age = 10, m.age = 11)";
        {
            const auto res = query(CREATE_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
        {
            const auto res = query(SET_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
    }
    {
        newChange(), changeB = _currentChange;
        constexpr std::string_view CREATE_QUERY =
            R"(CREATE (n:Person{name:"Cat", hasPhD:false})-[e:KNOWS{age:2}]->(m:Person{name:"Dan"}))";
        constexpr std::string_view SET_QUERY =
            R"(MATCH (n:Person)-[e:KNOWS]->(m:Person) WHERE n.name = "Cat" SET n.age = 20, m.age = 21)";
        {
            const auto res = query(CREATE_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
        {
            const auto res = query(SET_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
    }
    {
        newChange(), changeC = _currentChange;
        constexpr std::string_view CREATE_QUERY =
            R"(CREATE (n:Person{name:"Eve", hasPhD:true})-[e:KNOWS{age:3}]->(m:Person{name:"Fox"}))";
        constexpr std::string_view SET_QUERY =
            R"(MATCH (n:Person)-[e:KNOWS]->(m:Person) WHERE n.name = "Eve" SET n.age = 30, m.age = 31)";
        {
            const auto res = query(CREATE_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
            ASSERT_TRUE(query("commit", emptyCallback));
        }
        {
            const auto res = query(SET_QUERY, emptyCallback);
            ASSERT_TRUE(res) << res.getError();
        }
    }
    submitChange(changeC);
    submitChange(changeB);
    submitChange(changeA);
    {
        constexpr std::string_view MATCH_QUERY =
            R"(MATCH (n:Person)-[e:KNOWS]->(m:Person) RETURN n, n.name, n.age, n.hasPhD, m, m.name, m.age, e.age)";
        const auto res = query(MATCH_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(8, df->size());

            const auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            const auto* nNames = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* nAges = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* hasPhDs = findColumn(df, "n.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            const auto* ms = findColumn(df, "m")->as<ColumnNodeIDs>();
            const auto* mNames = findColumn(df, "m.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* mAges = findColumn(df, "m.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* eAges = findColumn(df, "e.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(ns && nNames && nAges && hasPhDs && ms && mNames && mAges && eAges);
            ASSERT_FALSE(ns->empty());
            EXPECT_EQ((std::vector<NodeID>{ 0, 2, 4 }), ns->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Eve", "Cat", "Ava" }), nNames->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Int64::Primitive>>{ 30, 20, 10}), nAges->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Bool::Primitive>>{ true, false, true }), hasPhDs->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<NodeID>{ 1, 3, 5 }), ms->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<std::string_view>>{ "Fox", "Dan", "Ben" }), mNames->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Int64::Primitive>>{ 31, 21, 11 }), mAges->getRaw()) << dump(df);
            EXPECT_EQ((std::vector<std::optional<types::Int64::Primitive>>{ 3, 2, 1 }), eAges->getRaw()) << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 100;
    });
}
