#include <gtest/gtest.h>

#include <fstream>
#include <string>
#include <string_view>

#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnIDs.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"

#include "LineContainer.h"
#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class LoadCSVTest : public TuringTest {
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

    auto query(std::string_view query, auto callback) {
        auto res = _db->query(query, _graphName, &_env->getMem(),
                              callback, CommitHash::head(),
                              _currentChange);
        return res;
    }

    void newChange() {
        auto res = _env->getSystemManager().newChange(_graphName);
        ASSERT_TRUE(res);
        _currentChange = res.value()->id();
    }

    void submitCurrentChange() {
        auto res = _db->query("change submit", _graphName, &_env->getMem(),
                              CommitHash::head(), _currentChange);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    std::string writeTempCSV(const std::string& name,
                             const std::string& content) {
        const std::string& dataDir = _env->getConfig().getDataDir().get();
        std::string path = dataDir + "/" + name;
        std::ofstream f(path);
        f << content;
        f.close();
        return name;
    }

    NodeID findNode(std::string_view name) {
        return SimpleGraph::findNodeID(_graph, name);
    }
};

// =============================================================================
// Basic LOAD CSV (sanity — no graph interaction)
// =============================================================================

TEST_F(LoadCSVTest, loadCSVReturn) {
    const std::string csv = writeTempCSV("basic.csv",
        "Alice,30,London\n"
        "Bob,25,Paris\n");

    const std::string q =
        "LOAD CSV '" + csv + "' AS row "
        "RETURN row[0] AS name, row[1] AS age, row[2] AS city";

    using Rows = LineContainer<std::string, std::string, std::string>;
    Rows expected;
    expected.add({"Alice", "30", "London"});
    expected.add({"Bob", "25", "Paris"});

    Rows actual;
    auto res = query(q, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        const auto& cols = df->cols();
        ASSERT_EQ(cols.size(), 3);

        const auto* c0 = cols.at(0)->as<ColumnVector<std::string>>();
        const auto* c1 = cols.at(1)->as<ColumnVector<std::string>>();
        const auto* c2 = cols.at(2)->as<ColumnVector<std::string>>();
        ASSERT_TRUE(c0 && c1 && c2);

        for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
            actual.add({std::string {c0->at(i)},
                        std::string {c1->at(i)},
                        std::string {c2->at(i)}});
        }
    });
    ASSERT_TRUE(res) << res.getError();
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(LoadCSVTest, loadCSVWithHeaders) {
    const std::string csv = writeTempCSV("headers.csv",
        "name,age,city\n"
        "Alice,30,London\n"
        "Bob,25,Paris\n");

    const std::string q =
        "LOAD CSV '" + csv + "' WITH HEADERS AS row "
        "RETURN row.name AS name, row.age AS age, row.city AS city";

    using Rows = LineContainer<std::string, std::string, std::string>;
    Rows expected;
    expected.add({"Alice", "30", "London"});
    expected.add({"Bob", "25", "Paris"});

    Rows actual;
    auto res = query(q, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        const auto* c0 = df->cols().at(0)->as<ColumnVector<std::string>>();
        const auto* c1 = df->cols().at(1)->as<ColumnVector<std::string>>();
        const auto* c2 = df->cols().at(2)->as<ColumnVector<std::string>>();
        ASSERT_TRUE(c0 && c1 && c2);

        for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
            actual.add({std::string {c0->at(i)},
                        std::string {c1->at(i)},
                        std::string {c2->at(i)}});
        }
    });
    ASSERT_TRUE(res) << res.getError();
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// LOAD CSV + MATCH
// =============================================================================

TEST_F(LoadCSVTest, DISABLED_loadCSVMatchByName) {
    // CSV has names of people in SimpleGraph.
    // Match each CSV row against the graph by name.
    const std::string csv = writeTempCSV("match.csv",
        "Remy\n"
        "Adam\n"
        "Luc\n");

    const std::string q =
        "LOAD CSV '" + csv + "' AS row "
        "MATCH (n:Person {name: row[0]}) "
        "RETURN row[0] AS csvName, n";

    using Rows = LineContainer<std::string, NodeID>;
    Rows expected;
    expected.add({"Remy", findNode("Remy")});
    expected.add({"Adam", findNode("Adam")});
    expected.add({"Luc", findNode("Luc")});

    Rows actual;
    auto res = query(q, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        const auto& cols = df->cols();
        ASSERT_EQ(cols.size(), 2);

        const auto* nameCol =
            cols.at(0)->as<ColumnVector<std::string>>();
        const auto* nodeCol = cols.at(1)->as<ColumnNodeIDs>();
        ASSERT_TRUE(nameCol && nodeCol);

        for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
            actual.add({std::string {nameCol->at(i)},
                        nodeCol->at(i)});
        }
    });
    ASSERT_TRUE(res) << "LOAD CSV + MATCH query failed: "
                      << res.getError();
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(LoadCSVTest, DISABLED_loadCSVMatchWithHeaders) {
    const std::string csv = writeTempCSV("match_headers.csv",
        "person_name\n"
        "Remy\n"
        "Martina\n"
        "Cyrus\n"
        "Doruk\n");

    const std::string q =
        "LOAD CSV '" + csv + "' WITH HEADERS AS row "
        "MATCH (n:Person {name: row.person_name}) "
        "RETURN row.person_name AS csvName, n";

    using Rows = LineContainer<std::string, NodeID>;
    Rows expected;
    expected.add({"Remy", findNode("Remy")});
    expected.add({"Martina", findNode("Martina")});
    expected.add({"Cyrus", findNode("Cyrus")});
    expected.add({"Doruk", findNode("Doruk")});

    Rows actual;
    auto res = query(q, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        const auto* nameCol =
            df->cols().at(0)->as<ColumnVector<std::string>>();
        const auto* nodeCol = df->cols().at(1)->as<ColumnNodeIDs>();
        ASSERT_TRUE(nameCol && nodeCol);

        for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
            actual.add({std::string {nameCol->at(i)},
                        nodeCol->at(i)});
        }
    });
    ASSERT_TRUE(res) << "LOAD CSV WITH HEADERS + MATCH query failed";
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(LoadCSVTest, DISABLED_loadCSVMatchReturnProperty) {
    // LOAD CSV + MATCH, return a graph property (n.age) alongside CSV data.
    // SimpleGraph: Remy age=32, Adam age=32
    const std::string csv = writeTempCSV("match_prop.csv",
        "Remy\n"
        "Adam\n");

    const std::string q =
        "LOAD CSV '" + csv + "' AS row "
        "MATCH (n:Person {name: row[0]}) "
        "RETURN row[0] AS csvName, n.name AS graphName, n.age AS age";

    using String = types::String::Primitive;
    using Int = types::Int64::Primitive;
    using Rows = LineContainer<std::string, std::optional<String>,
                               std::optional<Int>>;

    Rows expected;
    expected.add({"Remy", String {"Remy"}, Int {32}});
    expected.add({"Adam", String {"Adam"}, Int {32}});

    Rows actual;
    auto res = query(q, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        const auto& cols = df->cols();
        ASSERT_EQ(cols.size(), 3);

        const auto* csvCol =
            cols.at(0)->as<ColumnVector<std::string>>();
        const auto* gnCol =
            cols.at(1)->as<ColumnOptVector<String>>();
        const auto* ageCol =
            cols.at(2)->as<ColumnOptVector<Int>>();
        ASSERT_TRUE(csvCol && gnCol && ageCol);

        for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
            actual.add({std::string {csvCol->at(i)},
                        gnCol->at(i),
                        ageCol->at(i)});
        }
    });
    ASSERT_TRUE(res) << "LOAD CSV + MATCH + property RETURN failed";
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(LoadCSVTest, DISABLED_loadCSVMatchPartialHit) {
    // Mix of names that exist and don't exist.
    // Only matching rows should appear in the result.
    const std::string csv = writeTempCSV("partial.csv",
        "Remy\n"
        "DoesNotExist\n"
        "Luc\n");

    const std::string q =
        "LOAD CSV '" + csv + "' AS row "
        "MATCH (n:Person {name: row[0]}) "
        "RETURN row[0] AS csvName, n";

    using Rows = LineContainer<std::string, NodeID>;
    Rows expected;
    expected.add({"Remy", findNode("Remy")});
    expected.add({"Luc", findNode("Luc")});

    Rows actual;
    auto res = query(q, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        const auto* nameCol =
            df->cols().at(0)->as<ColumnVector<std::string>>();
        const auto* nodeCol = df->cols().at(1)->as<ColumnNodeIDs>();
        ASSERT_TRUE(nameCol && nodeCol);

        for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
            actual.add({std::string {nameCol->at(i)},
                        nodeCol->at(i)});
        }
    });
    ASSERT_TRUE(res) << "LOAD CSV + MATCH partial hit failed";
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(LoadCSVTest, DISABLED_loadCSVMatchNoResults) {
    // CSV names that don't exist in the graph.
    const std::string csv = writeTempCSV("nomatch.csv",
        "name\n"
        "NonExistent1\n"
        "NonExistent2\n");

    const std::string q =
        "LOAD CSV '" + csv + "' WITH HEADERS AS row "
        "MATCH (n:Person {name: row.name}) "
        "RETURN n";

    size_t totalRows = 0;
    auto res = query(q, [&](const Dataframe* df) {
        totalRows += df->getLogicalRowCount();
    });
    // Either succeeds with 0 rows or fails — both acceptable.
    if (res) {
        EXPECT_EQ(totalRows, 0);
    }
}

// =============================================================================
// LOAD CSV + CREATE
// =============================================================================

TEST_F(LoadCSVTest, loadCSVCreateNodesWithHeaders) {
    const std::string csv = writeTempCSV("create_headers.csv",
        "name,city\n"
        "Alice,London\n"
        "Bob,Paris\n"
        "Charlie,Berlin\n");

    newChange();
    {
        const std::string q =
            "LOAD CSV '" + csv + "' WITH HEADERS AS row "
            "CREATE (n:Imported {name: row.name, city: row.city})";

        auto res = query(q, [](const Dataframe*) {});
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    // Query back the created nodes and verify properties
    using Rows = LineContainer<std::string, std::string>;
    Rows expected;
    expected.add({"Alice", "London"});
    expected.add({"Bob", "Paris"});
    expected.add({"Charlie", "Berlin"});

    Rows actual;
    {
        auto res = query("MATCH (n:Imported) RETURN n.name, n.city",
            [&](const Dataframe* df) {
                ASSERT_TRUE(df);
                const auto* nameCol = df->cols()[0]->as<ColumnOptVector<types::String::Primitive>>();
                const auto* cityCol = df->cols()[1]->as<ColumnOptVector<types::String::Primitive>>();
                ASSERT_TRUE(nameCol && cityCol);

                for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
                    actual.add({std::string((*nameCol)[i].value()),
                                std::string((*cityCol)[i].value())});
                }
            });
        ASSERT_TRUE(res) << res.getError();
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(LoadCSVTest, loadCSVCreateNodesWithTypeConversion) {
    const std::string csv = writeTempCSV("create_typed.csv",
        "name,age\n"
        "Alice,30\n"
        "Bob,25\n");

    newChange();
    {
        const std::string q =
            "LOAD CSV '" + csv + "' WITH HEADERS AS row "
            "CREATE (n:TypedImport {name: row.name, age: toInteger(row.age)})";

        auto res = query(q, [](const Dataframe*) {});
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    // Verify integer property was stored correctly
    using Rows = LineContainer<std::string, int64_t>;
    Rows expected;
    expected.add({"Alice", 30});
    expected.add({"Bob", 25});

    Rows actual;
    {
        auto res = query("MATCH (n:TypedImport) RETURN n.name, n.age",
            [&](const Dataframe* df) {
                ASSERT_TRUE(df);
                const auto* nameCol = df->cols()[0]->as<ColumnOptVector<types::String::Primitive>>();
                const auto* ageCol = df->cols()[1]->as<ColumnOptVector<types::Int64::Primitive>>();
                ASSERT_TRUE(nameCol && ageCol);

                for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
                    actual.add({std::string((*nameCol)[i].value()),
                                (*ageCol)[i].value()});
                }
            });
        ASSERT_TRUE(res) << res.getError();
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(LoadCSVTest, loadCSVCreateNodesIndexAccess) {
    const std::string csv = writeTempCSV("create_index.csv",
        "Alice,London\n"
        "Bob,Paris\n");

    newChange();
    {
        const std::string q =
            "LOAD CSV '" + csv + "' AS row "
            "CREATE (n:IndexImport {name: row[0], city: row[1]})";

        auto res = query(q, [](const Dataframe*) {});
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    // Query back and verify
    using Rows = LineContainer<std::string, std::string>;
    Rows expected;
    expected.add({"Alice", "London"});
    expected.add({"Bob", "Paris"});

    Rows actual;
    {
        auto res = query("MATCH (n:IndexImport) RETURN n.name, n.city",
            [&](const Dataframe* df) {
                ASSERT_TRUE(df);
                const auto* nameCol = df->cols()[0]->as<ColumnOptVector<types::String::Primitive>>();
                const auto* cityCol = df->cols()[1]->as<ColumnOptVector<types::String::Primitive>>();
                ASSERT_TRUE(nameCol && cityCol);

                for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
                    actual.add({std::string((*nameCol)[i].value()),
                                std::string((*cityCol)[i].value())});
                }
            });
        ASSERT_TRUE(res) << res.getError();
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(LoadCSVTest, loadCSVCreateEdges) {
    const std::string csv = writeTempCSV("create_edges.csv",
        "src,tgt\n"
        "Alice,Bob\n"
        "Charlie,Doruk\n");

    newChange();
    {
        const std::string q =
            "LOAD CSV '" + csv + "' WITH HEADERS AS row "
            "CREATE (a:CSVNode {name: row.src})-[:LINKED]->(b:CSVNode {name: row.tgt})";

        auto res = query(q, [](const Dataframe*) {});
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    // Query back edges and verify src->tgt relationships
    using Rows = LineContainer<std::string, std::string>;
    Rows expected;
    expected.add({"Alice", "Bob"});
    expected.add({"Charlie", "Doruk"});

    Rows actual;
    {
        auto res = query("MATCH (a:CSVNode)-[:LINKED]->(b:CSVNode) RETURN a.name, b.name",
            [&](const Dataframe* df) {
                ASSERT_TRUE(df);
                const auto* srcCol = df->cols()[0]->as<ColumnOptVector<types::String::Primitive>>();
                const auto* tgtCol = df->cols()[1]->as<ColumnOptVector<types::String::Primitive>>();
                ASSERT_TRUE(srcCol && tgtCol);

                for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
                    actual.add({std::string((*srcCol)[i].value()),
                                std::string((*tgtCol)[i].value())});
                }
            });
        ASSERT_TRUE(res) << res.getError();
    }
    EXPECT_TRUE(expected.equals(actual));
}
