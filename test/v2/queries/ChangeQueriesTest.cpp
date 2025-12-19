#include <gtest/gtest.h>
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


    GraphReader read() { return _graph->openTransaction().readGraph(); }

    void newChange() {
        auto res = _env->getSystemManager().newChange(_graphName);
        ASSERT_TRUE(res);

        Change* change = res.value();
        _currentChange = change->id();
    }

    void submitCurrentChange() {
        auto res = _db->query("change submit", _graphName, &_env->getMem(), CommitHash::head(), _currentChange);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    void submitChange(ChangeID chid) {
        auto res = _db->query("change submit", _graphName, &_env->getMem(), CommitHash::head(), chid);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
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
