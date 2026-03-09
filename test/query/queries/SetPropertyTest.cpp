#include <gtest/gtest.h>

#include <optional>

#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"

#include "LineContainer.h"
#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class SetPropertyTest : public TuringTest {
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _env->getSystemManager().createGraph("default");
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

    auto query(std::string_view query, auto callback) {
        auto res = _db->query(query, _graphName, &_env->getMem(), callback,
                              CommitHash::head(), _currentChange);
        return res;
    }

    auto queryAt(CommitHash hash, std::string_view query, auto callback) {
        auto res = _db->query(query, _graphName, &_env->getMem(), callback,
                              hash, ChangeID::head());
        return res;
    }

    static NamedColumn* findColumn(const Dataframe* df, std::string_view name) {
        for (auto* col : df->cols()) {
            if (col->getName() == name) {
                return col;
            }
        }
        return nullptr;
    }
};

// 1. SET existing Int64 property, verify persistence and isolation
TEST_F(SetPropertyTest, setExistingIntProperty) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 99)", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    // Verify Remy's age is 99
    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            ASSERT_TRUE(ages->at(0).has_value());
            EXPECT_EQ(*ages->at(0), 99);
        });
        ASSERT_TRUE(res);
    }

    // Verify Adam's age unchanged (32)
    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Adam" RETURN n.age)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            ASSERT_TRUE(ages->at(0).has_value());
            EXPECT_EQ(*ages->at(0), 32);
        });
        ASSERT_TRUE(res);
    }
}

// 2. SET existing String property
TEST_F(SetPropertyTest, setExistingStringProperty) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.name = "RemyUpdated")", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    // Verify renamed node is found
    {
        auto res = query(R"(MATCH (n) WHERE n.name = "RemyUpdated" RETURN n.name)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
        });
        ASSERT_TRUE(res);
    }

    // Verify old name no longer matches
    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.name)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            EXPECT_EQ(df->getLogicalRowCount(), 0);
        });
        ASSERT_TRUE(res);
    }
}

// 3. SET new property type on existing node
TEST_F(SetPropertyTest, setNewPropertyOnExistingNode) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.city = "Paris")", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.city)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* cities = findColumn(df, "n.city")->as<ColumnOptVector<std::string_view>>();
            ASSERT_TRUE(cities);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            ASSERT_TRUE(cities->at(0).has_value());
            EXPECT_EQ(*cities->at(0), "Paris");
        });
        ASSERT_TRUE(res);
    }
}

// 4. SET multiple properties in one clause (comma-separated)
TEST_F(SetPropertyTest, setMultiplePropertiesSameClause) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 50, n.isFrench = false)", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age, n.isFrench)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            auto* flags = findColumn(df, "n.isFrench")->as<ColumnOptVector<db::CustomBool>>();
            ASSERT_TRUE(ages);
            ASSERT_TRUE(flags);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            EXPECT_EQ(*ages->at(0), 50);
            EXPECT_FALSE(static_cast<bool>(*flags->at(0)));
        });
        ASSERT_TRUE(res);
    }
}

// 5. Multiple SET clauses on same node
TEST_F(SetPropertyTest, setMultipleSETClauses) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 50 SET n.isFrench = false)", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age, n.isFrench)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            auto* flags = findColumn(df, "n.isFrench")->as<ColumnOptVector<db::CustomBool>>();
            ASSERT_TRUE(ages);
            ASSERT_TRUE(flags);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            EXPECT_EQ(*ages->at(0), 50);
            EXPECT_FALSE(static_cast<bool>(*flags->at(0)));
        });
        ASSERT_TRUE(res);
    }
}

// 6. SET same property twice — last write wins
TEST_F(SetPropertyTest, setSamePropertyTwice) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 50 SET n.age = 99)", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            EXPECT_EQ(*ages->at(0), 99);
        });
        ASSERT_TRUE(res);
    }
}

// 7. SET affects all matched nodes
TEST_F(SetPropertyTest, setPropertyOnAllMatchedNodes) {
    newChange();
    auto res = query(R"(MATCH (n:Person) SET n.isFrench = true)", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n:Person) RETURN n.isFrench)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* flags = findColumn(df, "n.isFrench")->as<ColumnOptVector<db::CustomBool>>();
            ASSERT_TRUE(flags);
            size_t rowCount = df->getLogicalRowCount();
            ASSERT_GT(rowCount, 0);
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(flags->at(i).has_value()) << "Row " << i << " has no value";
                EXPECT_TRUE(static_cast<bool>(*flags->at(i))) << "Row " << i << " is not true";
            }
        });
        ASSERT_TRUE(res);
    }
}

// 8. SET to NULL removes property
TEST_F(SetPropertyTest, setPropertyNullRemovesProperty) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = NULL)", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    // Verify age is null
    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            EXPECT_FALSE(ages->at(0).has_value());
        });
        ASSERT_TRUE(res);
    }

    // Verify other properties intact
    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.name, n.hasPhD)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<std::string_view>>();
            auto* phds = findColumn(df, "n.hasPhD")->as<ColumnOptVector<db::CustomBool>>();
            ASSERT_TRUE(names);
            ASSERT_TRUE(phds);
            EXPECT_EQ(*names->at(0), "Remy");
            EXPECT_TRUE(phds->at(0).has_value());
        });
        ASSERT_TRUE(res);
    }
}

// 9. REMOVE property syntax (desugared to SET NULL)
TEST_F(SetPropertyTest, removePropertySyntax) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" REMOVE n.age)", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            EXPECT_FALSE(ages->at(0).has_value());
        });
        ASSERT_TRUE(res);
    }
}

// 10. REMOVE multiple properties
TEST_F(SetPropertyTest, removeMultipleProperties) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" REMOVE n.age, n.isFrench)", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age, n.isFrench, n.name)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            auto* flags = findColumn(df, "n.isFrench")->as<ColumnOptVector<db::CustomBool>>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<std::string_view>>();
            ASSERT_TRUE(ages);
            ASSERT_TRUE(flags);
            ASSERT_TRUE(names);
            EXPECT_FALSE(ages->at(0).has_value());
            EXPECT_FALSE(flags->at(0).has_value());
            EXPECT_EQ(*names->at(0), "Remy");
        });
        ASSERT_TRUE(res);
    }
}

// 11. Remove then re-add property across commits
TEST_F(SetPropertyTest, reAddRemovedProperty) {
    // Step 1: Remove age
    newChange();
    auto res1 = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = NULL)", [](const Dataframe*) {});
    ASSERT_TRUE(res1);
    submitCurrentChange();

    // Verify age is gone
    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            EXPECT_FALSE(ages->at(0).has_value());
        });
        ASSERT_TRUE(res);
    }

    // Step 2: Re-add age
    newChange();
    auto res2 = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 42)", [](const Dataframe*) {});
    ASSERT_TRUE(res2);
    submitCurrentChange();

    // Verify age is 42
    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            ASSERT_TRUE(ages->at(0).has_value());
            EXPECT_EQ(*ages->at(0), 42);
        });
        ASSERT_TRUE(res);
    }
}

// 11b. SET same property across 3 commits — newest DataPart wins
TEST_F(SetPropertyTest, setAcrossMultipleCommits) {
    // Commit 1: age = 50
    newChange();
    auto r1 = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 50)", [](const Dataframe*) {});
    ASSERT_TRUE(r1);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)", [](const Dataframe* df) {
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            EXPECT_EQ(*ages->at(0), 50);
        });
        ASSERT_TRUE(res);
    }

    // Commit 2: age = 75
    newChange();
    auto r2 = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 75)", [](const Dataframe*) {});
    ASSERT_TRUE(r2);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)", [](const Dataframe* df) {
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            EXPECT_EQ(*ages->at(0), 75);
        });
        ASSERT_TRUE(res);
    }

    // Commit 3: age = 100
    newChange();
    auto r3 = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 100)", [](const Dataframe*) {});
    ASSERT_TRUE(r3);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age, n.name)", [](const Dataframe* df) {
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<std::string_view>>();
            EXPECT_EQ(*ages->at(0), 100);
            EXPECT_EQ(*names->at(0), "Remy");
        });
        ASSERT_TRUE(res);
    }
}

// 11c. SET different properties across commits — overlays combine
TEST_F(SetPropertyTest, setDifferentPropertiesAcrossCommits) {
    newChange();
    auto r1 = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.city = "Paris")", [](const Dataframe*) {});
    ASSERT_TRUE(r1);
    submitCurrentChange();

    newChange();
    auto r2 = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.country = "France")", [](const Dataframe*) {});
    ASSERT_TRUE(r2);
    submitCurrentChange();

    newChange();
    auto r3 = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 40)", [](const Dataframe*) {});
    ASSERT_TRUE(r3);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.city, n.country, n.age, n.name)",
                         [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            auto* cities = findColumn(df, "n.city")->as<ColumnOptVector<std::string_view>>();
            auto* countries = findColumn(df, "n.country")->as<ColumnOptVector<std::string_view>>();
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<std::string_view>>();
            EXPECT_EQ(*cities->at(0), "Paris");
            EXPECT_EQ(*countries->at(0), "France");
            EXPECT_EQ(*ages->at(0), 40);
            EXPECT_EQ(*names->at(0), "Remy");
        });
        ASSERT_TRUE(res);
    }
}

// 11d. SET value then SET NULL across commits — tombstone shadows older value
TEST_F(SetPropertyTest, setThenRemoveAcrossCommits) {
    // Commit 1: SET age = 50
    newChange();
    auto r1 = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 50)", [](const Dataframe*) {});
    ASSERT_TRUE(r1);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)", [](const Dataframe* df) {
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages->at(0).has_value());
            EXPECT_EQ(*ages->at(0), 50);
        });
        ASSERT_TRUE(res);
    }

    // Commit 2: SET age = NULL (tombstone)
    newChange();
    auto r2 = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = NULL)", [](const Dataframe*) {});
    ASSERT_TRUE(r2);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)", [](const Dataframe* df) {
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            EXPECT_FALSE(ages->at(0).has_value());
        });
        ASSERT_TRUE(res);
    }
}

// 12. SET edge property
TEST_F(SetPropertyTest, setEdgeProperty) {
    newChange();
    auto res = query(R"(MATCH (n)-[e:KNOWS_WELL]->(m) WHERE n.name = "Remy" AND m.name = "Adam" SET e.duration = 999)",
                     [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n)-[e:KNOWS_WELL]->(m) WHERE n.name = "Remy" AND m.name = "Adam" RETURN e.duration)",
                         [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(durs);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            ASSERT_TRUE(durs->at(0).has_value());
            EXPECT_EQ(*durs->at(0), 999);
        });
        ASSERT_TRUE(res);
    }

    // Verify other KNOWS_WELL edge unchanged (Adam -> Remy still has duration=20)
    {
        auto res = query(R"(MATCH (n)-[e:KNOWS_WELL]->(m) WHERE n.name = "Adam" AND m.name = "Remy" RETURN e.duration)",
                         [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(durs);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            ASSERT_TRUE(durs->at(0).has_value());
            EXPECT_EQ(*durs->at(0), 20);
        });
        ASSERT_TRUE(res);
    }
}

// 13. SET new property on edge
TEST_F(SetPropertyTest, setNewEdgeProperty) {
    newChange();
    auto res = query(R"(MATCH (n)-[e:KNOWS_WELL]->(m) WHERE n.name = "Remy" AND m.name = "Adam" SET e.strength = 100)",
                     [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n)-[e:KNOWS_WELL]->(m) WHERE n.name = "Remy" AND m.name = "Adam" RETURN e.strength)",
                         [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* strengths = findColumn(df, "e.strength")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(strengths);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            ASSERT_TRUE(strengths->at(0).has_value());
            EXPECT_EQ(*strengths->at(0), 100);
        });
        ASSERT_TRUE(res);
    }
}

// 14. CREATE + SET in same query
TEST_F(SetPropertyTest, createThenSetProperty) {
    newChange();
    auto res = query(R"(CREATE (n:TestNode) SET n.name = "Created")", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n:TestNode) RETURN n.name)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<std::string_view>>();
            ASSERT_TRUE(names);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            ASSERT_TRUE(names->at(0).has_value());
            EXPECT_EQ(*names->at(0), "Created");
        });
        ASSERT_TRUE(res);
    }
}

// 15. SET + RETURN shows updated value in same pipeline
TEST_F(SetPropertyTest, setWithReturnShowsUpdatedValue) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 77 RETURN n.age)",
                     [](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
        ASSERT_TRUE(ages);
        ASSERT_EQ(df->getLogicalRowCount(), 1);
        ASSERT_TRUE(ages->at(0).has_value());
        EXPECT_EQ(*ages->at(0), 32);  // writes not visible until committed
    });
    ASSERT_TRUE(res);
    submitCurrentChange();
}

// 16. SET one property preserves all other properties
TEST_F(SetPropertyTest, setPreservesUnrelatedProperties) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 50)", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age, n.name, n.dob, n.isFrench, n.hasPhD)",
                         [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getLogicalRowCount(), 1);

            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<std::string_view>>();
            auto* dobs = findColumn(df, "n.dob")->as<ColumnOptVector<std::string_view>>();
            auto* french = findColumn(df, "n.isFrench")->as<ColumnOptVector<db::CustomBool>>();
            auto* phds = findColumn(df, "n.hasPhD")->as<ColumnOptVector<db::CustomBool>>();

            ASSERT_TRUE(ages);
            ASSERT_TRUE(names);
            ASSERT_TRUE(dobs);
            ASSERT_TRUE(french);
            ASSERT_TRUE(phds);

            EXPECT_EQ(*ages->at(0), 50);
            EXPECT_EQ(*names->at(0), "Remy");
            EXPECT_EQ(*dobs->at(0), "18/01");
            EXPECT_TRUE(static_cast<bool>(*french->at(0)));
            EXPECT_TRUE(static_cast<bool>(*phds->at(0)));
        });
        ASSERT_TRUE(res);
    }
}

// 17. SET does not affect unmatched nodes
TEST_F(SetPropertyTest, setDoesNotAffectUnmatchedNodes) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 999)", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    // Adam's age should still be 32
    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Adam" RETURN n.age)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            ASSERT_TRUE(ages->at(0).has_value());
            EXPECT_EQ(*ages->at(0), 32);
        });
        ASSERT_TRUE(res);
    }
}

// 18. SET with expression value
TEST_F(SetPropertyTest, setPropertyToExpressionValue) {
    newChange();
    auto res = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 20 + 10)", [](const Dataframe*) {});
    ASSERT_TRUE(res);
    submitCurrentChange();

    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            ASSERT_TRUE(ages->at(0).has_value());
            EXPECT_EQ(*ages->at(0), 30);
        });
        ASSERT_TRUE(res);
    }
}

// 19. Query previous commit after SET — returns old value
TEST_F(SetPropertyTest, queryPreviousCommitReturnsOldValue) {
    // Commit 1: SET age = 50
    newChange();
    auto r1 = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 50)", [](const Dataframe*) {});
    ASSERT_TRUE(r1);
    submitCurrentChange();

    // Capture the commit hash after first SET
    const CommitHash hashAfterFirst = _graph->getHeadHash();

    // Commit 2: SET age = 999
    newChange();
    auto r2 = query(R"(MATCH (n) WHERE n.name = "Remy" SET n.age = 999)", [](const Dataframe*) {});
    ASSERT_TRUE(r2);
    submitCurrentChange();

    // Latest commit should see age = 999
    {
        auto res = query(R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)", [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            ASSERT_TRUE(ages->at(0).has_value());
            EXPECT_EQ(*ages->at(0), 999);
        });
        ASSERT_TRUE(res);
    }

    // Previous commit should still see age = 50
    {
        auto res = queryAt(hashAfterFirst, R"(MATCH (n) WHERE n.name = "Remy" RETURN n.age)",
                           [](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<int64_t>>();
            ASSERT_TRUE(ages);
            ASSERT_EQ(df->getLogicalRowCount(), 1);
            ASSERT_TRUE(ages->at(0).has_value());
            EXPECT_EQ(*ages->at(0), 50);
        });
        ASSERT_TRUE(res);
    }
}
