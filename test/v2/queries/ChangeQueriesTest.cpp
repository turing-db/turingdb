#include <cstdint>
#include <gtest/gtest.h>
#include <initializer_list>
#include <string_view>

#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/Block.h"
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

    GraphReader read() { return _graph->openTransaction().readGraph(); }

    void newChange() {
        auto res = _env->getSystemManager().newChange(_graphName);
        ASSERT_TRUE(res);

        Change* change = res.value();
        _currentChange = change->id();
    }

    void submitCurrentChange() {
        auto res = _db->query("CHANGE SUBMIT", _graphName, &_env->getMem(), CommitHash::head(), _currentChange);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    void submitChange(ChangeID chid) {
        auto res = _db->query("CHANGE SUBMIT", _graphName, &_env->getMem(), CommitHash::head(), chid);

        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    void setChange(ChangeID chid) {
        _currentChange = chid;
    }

    auto queryV2(std::string_view query, auto callback) {
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

    bool ensureProperties(std::initializer_list<std::string_view> props) {
        bool allPropsFound {true};
        
        auto res = queryV2(GET_PROPERTIES_QUERY, [&](const Dataframe* df) {
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
        ASSERT_TRUE(queryV2(CREATE_QUERY, [](const Dataframe*){}));
    }
    {
        newChange(), change2 = _currentChange;
        std::string_view CREATE_QUERY =
            R"(CREATE (n:TestNode2 { name: "3" })-[e:TestEdge2 { name: "3->4" }]->(m:TestNode2 { name: "4" }))";
        ASSERT_TRUE(queryV2(CREATE_QUERY, [](const Dataframe*){}));
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
        auto res = queryV2(MATCH_QUERY, [&actual](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(3, df->size());

            Name nName;
            Name eName;
            Name mName;
            for (auto* namedCol : df->cols()) {
                ASSERT_TRUE(namedCol);
                ASSERT_TRUE(namedCol->getColumn());
                ASSERT_EQ(1, namedCol->getColumn()->size());

                if (namedCol->getName() == "n.name") {
                    auto nNameOpt =
                        namedCol->getColumn()->cast<ColumnOptVector<Name>>()->front();
                    ASSERT_TRUE(nNameOpt);
                    nName = *nNameOpt;
                } else if (namedCol->getName() == "m.name") {
                    auto mNameOpt =
                        namedCol->getColumn()->cast<ColumnOptVector<Name>>()->front();
                    ASSERT_TRUE(mNameOpt);
                    mName = *mNameOpt;
                } else if (namedCol->getName() == "e.name") {
                    auto eNameOpt =
                        namedCol->getColumn()->cast<ColumnOptVector<Name>>()->front();
                    ASSERT_TRUE(eNameOpt);
                    eName = *eNameOpt;
                } else {
                    ASSERT_FALSE(true) << "Unexpected column in dataframe";
                }
            }

            actual.add({nName, eName, mName});
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
        auto res = queryV2(MATCH_QUERY, [&actual](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(3, df->size());

            Name nName;
            Name eName;
            Name mName;
            for (auto* namedCol : df->cols()) {
                ASSERT_TRUE(namedCol);
                ASSERT_TRUE(namedCol->getColumn());
                ASSERT_EQ(1, namedCol->getColumn()->size());

                if (namedCol->getName() == "n.name") {
                    auto nNameOpt = namedCol->getColumn()->cast<ColumnOptVector<Name>>()->front();
                    ASSERT_TRUE(nNameOpt);
                    nName = *nNameOpt;
                } else if (namedCol->getName() == "m.name") {
                    auto mNameOpt = namedCol->getColumn()->cast<ColumnOptVector<Name>>()->front();
                    ASSERT_TRUE(mNameOpt);
                    mName = *mNameOpt;
                } else if (namedCol->getName() == "e.name") {
                    auto eNameOpt = namedCol->getColumn()->cast<ColumnOptVector<Name>>()->front();
                    ASSERT_TRUE(eNameOpt);
                    eName = *eNameOpt;
                } else {
                    ASSERT_FALSE(true) << "Unexpected column in dataframe";
                }
            }
            actual.add({nName, eName, mName});
        });
        ASSERT_TRUE(res);

        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(ChangeQueriesTest, threeChangeRebase) {
    setWorkingGraph("default");
    auto emptyCallback = [](const Dataframe*) -> void {};

    auto makeChanges = [&]() -> void {
        newChange();
        { // Change
            ASSERT_TRUE(queryV2("CREATE (n:NODE)", emptyCallback));
            ASSERT_TRUE(queryV2("COMMIT", emptyCallback));

            ASSERT_TRUE(queryV2("MATCH (n:NODE) CREATE (n)-[:EDGE]->(m:NODE)", emptyCallback));
            ASSERT_TRUE(queryV2("COMMIT", emptyCallback));
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
            auto res = queryV2("MATCH (n) RETURN n", [&actual](const Dataframe* df) {
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
            auto res = queryV2("MATCH (n)-[e]->(m) RETURN n,e,m", [&actual](const Dataframe* df) {
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
        ASSERT_TRUE(queryV2(
            R"(create (n:CHANGE1LABEL {id:1, changeid: "ONE", committed:true}))",
            [](const Dataframe*) {}));

        ASSERT_TRUE(queryV2("COMMIT", [](const Dataframe*) {}));

        ASSERT_TRUE(queryV2(
            R"(create (n:CHANGE1LABEL {cheeky:true, id:2, changeid: "ONE", committed:true}))",
            [](const Dataframe*) {}));

        ASSERT_TRUE(queryV2(
            R"(create (n:CHANGE1LABEL {cheeky:true, id:3, changeid: "ONE", committed:true}))",
            [](const Dataframe*) {}));

        ASSERT_TRUE(queryV2("COMMIT", [](const Dataframe*) {}));
        ASSERT_TRUE(ensureProperties({"id", "changeid", "committed"}));
    }

    newChange();
    [[maybe_unused]] ChangeID change2 = _currentChange;

    { // Change 2
        ASSERT_TRUE(queryV2(
            R"(create (n:CHANGE2LABEL {id:4, changeid: "TWO", committed:false}))",
            [](const Dataframe*) {}));
        ASSERT_TRUE(queryV2(
            R"(create (n:CHANGE2LABEL {id:5, changeid: "TWO", committed:false}))",
            [](const Dataframe*) {}));
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
            auto res = queryV2(matchQuery, [&actual](const Dataframe* df) {
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
            auto res = queryV2(matchQuery, [&actual](const Dataframe* df) {
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
