#include <gtest/gtest.h>

#include <functional>
#include <string_view>

#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "ID.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"

#include "LineContainer.h"
#include "TuringException.h"
#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class QueriesTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};

    GraphReader read() { return _graph->openTransaction().readGraph(); }

    // To test queries which require multiple changes, use WriteQueriesTest.cpp
    auto query(std::string_view query, auto callback) {
        auto res = _db->query(query, _graphName, &_env->getMem(), callback,
                              CommitHash::head(), ChangeID::head());
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

    PropertyTypeID getPropID(std::string_view propertyName) {
        auto propOpt = read().getView().metadata().propTypes().get(propertyName);
        if (!propOpt) {
            throw TuringException(
                fmt::format("Failed to get property: {}.", propertyName));
        }
        return propOpt->_id;
    }
};

template <typename T>
void assertSpanEqual(const std::span<const T>& a, const std::span<const T>& b) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
        ASSERT_EQ(a[i], b[i]);
    }
}

TEST_F(QueriesTest, scanAll) {
    const std::string query = "MATCH (n) RETURN n";

    std::vector<NodeID> returnedNodeIDs;
    _db->query(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 1);
        ASSERT_EQ(df->size(), 1);

        for (auto col : df->cols()) {
            const ColumnNodeIDs* nodeIDs = col->as<ColumnNodeIDs>();
            ASSERT_TRUE(nodeIDs != nullptr);
            ASSERT_FALSE(nodeIDs->empty());
            ASSERT_EQ(col->getName(), "n");
            returnedNodeIDs.insert(returnedNodeIDs.end(), nodeIDs->begin(), nodeIDs->end());
        }
    });

    // Get all expected node IDs
    std::vector<NodeID> expectedNodeIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            expectedNodeIDs.push_back(node);
        }
    }

    ASSERT_FALSE(returnedNodeIDs.empty());
    ASSERT_EQ(returnedNodeIDs, expectedNodeIDs);
}

TEST_F(QueriesTest, scanAllSkip) {
    // Get all expected node IDs
    std::vector<NodeID> allNodeIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            allNodeIDs.push_back(node);
        }
    }

    ASSERT_FALSE(allNodeIDs.empty());

    const size_t extraSkip = 10;
    const size_t maxSkip = allNodeIDs.size()+extraSkip;
    for (size_t skip = 0; skip <= maxSkip; ++skip) {
        const std::string query = "MATCH (n) RETURN n SKIP " + std::to_string(skip);

        std::vector<NodeID> returnedNodeIDs;
        bool executedLambda = false;
        _db->query(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
            executedLambda = true;
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->size(), 1);

            for (auto col : df->cols()) {
                const ColumnNodeIDs* nodeIDs = col->as<ColumnNodeIDs>();
                ASSERT_TRUE(nodeIDs != nullptr);
                ASSERT_FALSE(nodeIDs->empty());
                ASSERT_EQ(col->getName(), "n");
                returnedNodeIDs.insert(returnedNodeIDs.end(), nodeIDs->begin(), nodeIDs->end());
            }
        });

        if (skip >= allNodeIDs.size()) {
            ASSERT_FALSE(executedLambda);
            ASSERT_TRUE(returnedNodeIDs.empty());
            continue;
        }

        assertSpanEqual(std::span<const NodeID>(returnedNodeIDs),
                        std::span<const NodeID>(allNodeIDs).subspan(skip));
    }
}

TEST_F(QueriesTest, negativeSkip) {
    const std::string query = "MATCH (n) RETURN n SKIP -1";
    const auto result = _db->query(query, _graphName, &_env->getMem(), [](const Dataframe* df) -> void {});
    ASSERT_FALSE(result.isOk());
}

TEST_F(QueriesTest, scanAllLimit) {
    // Get all expected node IDs
    std::vector<NodeID> allNodeIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            allNodeIDs.push_back(node);
        }
    }

    ASSERT_FALSE(allNodeIDs.empty());

    const size_t extraLimit = 10;
    const size_t maxLimit = allNodeIDs.size()+extraLimit;
    for (size_t limit = 0; limit <= maxLimit; ++limit) {
        const std::string query = "MATCH (n) RETURN n LIMIT " + std::to_string(limit);

        std::vector<NodeID> returnedNodeIDs;
        bool executedLambda = false;
        _db->query(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
            executedLambda = true;
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->size(), 1);

            for (auto col : df->cols()) {
                const ColumnNodeIDs* nodeIDs = col->as<ColumnNodeIDs>();
                ASSERT_TRUE(nodeIDs != nullptr);
                ASSERT_FALSE(nodeIDs->empty());
                ASSERT_EQ(col->getName(), "n");
                returnedNodeIDs.insert(returnedNodeIDs.end(), nodeIDs->begin(), nodeIDs->end());
            }
        });

        const size_t limitCompare = std::min(limit, allNodeIDs.size());
        assertSpanEqual(std::span<const NodeID>(returnedNodeIDs),
                        std::span<const NodeID>(allNodeIDs).subspan(0, limitCompare));
    }
}

TEST_F(QueriesTest, scanAllSkipLimit) {
    // Get all expected node IDs
    std::vector<NodeID> allNodeIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            allNodeIDs.push_back(node);
        }
    }

    ASSERT_FALSE(allNodeIDs.empty());

    const size_t extra = 10;
    const size_t maxSkip = allNodeIDs.size()+extra;
    const size_t maxLimit = allNodeIDs.size()+extra;
    std::vector<NodeID> returnedNodeIDs;
    for (size_t skip = 0; skip <= maxSkip; ++skip) {
        for (size_t limit = 0; limit <= maxLimit; ++limit) {
            const std::string query = "MATCH (n) RETURN n SKIP " 
            + std::to_string(skip) + " LIMIT " 
            + std::to_string(limit);

            returnedNodeIDs.clear();
            bool executedLambda = false;
            _db->query(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                executedLambda = true;
                ASSERT_TRUE(df != nullptr);
                ASSERT_EQ(df->cols().size(), 1);
                ASSERT_EQ(df->size(), 1);

                const ColumnNodeIDs* nodeIDs = df->cols()[0]->as<ColumnNodeIDs>();
                ASSERT_TRUE(nodeIDs != nullptr);
                ASSERT_FALSE(nodeIDs->empty());
                returnedNodeIDs.insert(returnedNodeIDs.end(), nodeIDs->begin(), nodeIDs->end());
            });

            const size_t skipCompare = std::min(skip, allNodeIDs.size());
            const size_t limitCompare = std::min(limit, allNodeIDs.size()-skipCompare);
            assertSpanEqual(std::span<const NodeID>(returnedNodeIDs),
                            std::span<const NodeID>(allNodeIDs).subspan(skipCompare, limitCompare));
        }
    }
}

TEST_F(QueriesTest, scanExpand1) {
    const std::string queryStr = "MATCH (n)-[e]->(m) RETURN n, m, e";

    const size_t numEdges = read().getEdgeCount();

    LineContainer<NodeID, NodeID, EdgeID> returned;
    auto res = query(queryStr, [&returned, numEdges](const Dataframe* df) -> void {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 3);

        const auto* ns = df->cols().front()->as<ColumnNodeIDs>();
        const auto* ms = df->cols().at(1)->as<ColumnNodeIDs>();
        const auto* es = df->cols().back()->as<ColumnEdgeIDs>();

        ASSERT_TRUE(ns);
        ASSERT_TRUE(ms);
        ASSERT_TRUE(es);

        ASSERT_EQ(numEdges, ns->size());
        ASSERT_EQ(numEdges, ms->size());
        ASSERT_EQ(numEdges, es->size());
        for (size_t i = 0; i < numEdges; i++) {
            returned.add({ns->at(i), ms->at(i), es->at(i)});
        }
    });
    ASSERT_TRUE(res);

    LineContainer<NodeID, NodeID, EdgeID> expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
                expected.add({e._nodeID, e._otherID, e._edgeID});
        }
    }

    ASSERT_TRUE(expected.equals(returned));
}

TEST_F(QueriesTest, scanExpand2) {
    const std::string query = "MATCH (n)-->(m)-->(t) RETURN n, m, t";

    std::vector<NodeID> returnedTargets1;
    std::vector<NodeID> returnedTargets2;
    std::vector<NodeID> returnedSources;
    _db->query(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 3);
        ASSERT_EQ(df->size(), 3);

        const ColumnNodeIDs* targetIDs1 = nullptr;
        const ColumnNodeIDs* targetIDs2 = nullptr;
        const ColumnNodeIDs* sourceIDs = nullptr;
        for (auto col : df->cols()) {
            const auto name = col->getName();
            if (name == "m") {
                targetIDs1 = col->as<ColumnNodeIDs>();
            } else if (name == "t") {
                targetIDs2 = col->as<ColumnNodeIDs>();
            } else if (name == "n") {
                sourceIDs = col->as<ColumnNodeIDs>();
            }
        }

        ASSERT_TRUE(targetIDs1 != nullptr);
        ASSERT_FALSE(targetIDs1->empty());
        ASSERT_TRUE(targetIDs2 != nullptr);
        ASSERT_FALSE(targetIDs2->empty());
        ASSERT_TRUE(sourceIDs != nullptr);
        ASSERT_FALSE(sourceIDs->empty());
        returnedTargets1.insert(returnedTargets1.end(), targetIDs1->begin(), targetIDs1->end());
        returnedTargets2.insert(returnedTargets2.end(), targetIDs2->begin(), targetIDs2->end());
        returnedSources.insert(returnedSources.end(), sourceIDs->begin(), sourceIDs->end());
    });

    // Get all expected node IDs
    std::vector<NodeID> expectedSources;
    std::vector<NodeID> expectedTargets1;
    std::vector<NodeID> expectedTargets2;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            const auto edgeView1 = reader.getNodeView(node).edges();
            for (auto edge1 : edgeView1.outEdges()) {
                const auto edgeView2 = reader.getNodeView(edge1._otherID).edges();
                for (auto edge2 : edgeView2.outEdges()) {
                    expectedSources.push_back(node);
                    expectedTargets1.push_back(edge1._otherID);
                    expectedTargets2.push_back(edge2._otherID);
                }
            }
        }
    }

    ASSERT_FALSE(returnedSources.empty());
    ASSERT_FALSE(returnedTargets1.empty());
    ASSERT_FALSE(returnedTargets2.empty());
    ASSERT_EQ(returnedSources, expectedSources);
    ASSERT_EQ(returnedTargets1, expectedTargets1);
    ASSERT_EQ(returnedTargets2, expectedTargets2);
}

TEST_F(QueriesTest, scanExpandIn) {
    const std::string query = "MATCH (n)<--(m) RETURN n, m";

    LineContainer<NodeID, NodeID> returned;
    LineContainer<NodeID, NodeID> expected;
    _db->query(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 2);
        ASSERT_EQ(df->size(), 2);

        const ColumnNodeIDs* targetIDs = nullptr;
        const ColumnNodeIDs* sourceIDs = nullptr;
        for (auto col : df->cols()) {
            const auto name = col->getName();
            if (name == "m") {
                targetIDs = col->as<ColumnNodeIDs>();
            } else if (name == "n") {
                sourceIDs = col->as<ColumnNodeIDs>();
            }
        }

        ASSERT_TRUE(targetIDs != nullptr);
        ASSERT_FALSE(targetIDs->empty());
        ASSERT_TRUE(sourceIDs != nullptr);
        ASSERT_FALSE(sourceIDs->empty());

        const size_t lineCount = df->getRowCount();
        for (size_t i = 0; i < lineCount; i++) {
            returned.add({sourceIDs->at(i), targetIDs->at(i)});
        }
    });

    // Get all expected node IDs
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            const auto edgeView = reader.getNodeView(node).edges();
            for (auto edge : edgeView.inEdges()) {
                expected.add({edge._nodeID, edge._otherID});
            }
        }
    }

    ASSERT_TRUE(expected.equals(returned));
}

TEST_F(QueriesTest, scanExpandIn2) {
    const std::string query = "MATCH (n)<-[e1]-(m)<-[e2]-(t) RETURN n, e1, m, e2, t";

    LineContainer<NodeID, EdgeID, NodeID, EdgeID, NodeID> returnedLines;
    LineContainer<NodeID, EdgeID, NodeID, EdgeID, NodeID> expectedLines;
    _db->query(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 5);
        ASSERT_EQ(df->size(), 5);

        const ColumnNodeIDs* sourceIDs = nullptr;
        const ColumnEdgeIDs* edgeIDs1 = nullptr;
        const ColumnNodeIDs* targetIDs1 = nullptr;
        const ColumnEdgeIDs* edgeIDs2 = nullptr;
        const ColumnNodeIDs* targetIDs2 = nullptr;

        for (auto col : df->cols()) {
            const auto name = col->getName();
            if (name == "n") {
                sourceIDs = col->as<ColumnNodeIDs>();
            } else if (name == "e1") {
                edgeIDs1 = col->as<ColumnEdgeIDs>();
            } else if (name == "m") {
                targetIDs1 = col->as<ColumnNodeIDs>();
            } else if (name == "e2") {
                edgeIDs2 = col->as<ColumnEdgeIDs>();
            } else if (name == "t") {
                targetIDs2 = col->as<ColumnNodeIDs>();
            }
        }

        ASSERT_TRUE(sourceIDs != nullptr);
        ASSERT_FALSE(sourceIDs->empty());
        ASSERT_TRUE(edgeIDs1 != nullptr);
        ASSERT_FALSE(edgeIDs1->empty());
        ASSERT_TRUE(targetIDs1 != nullptr);
        ASSERT_FALSE(targetIDs1->empty());
        ASSERT_TRUE(edgeIDs2 != nullptr);
        ASSERT_FALSE(edgeIDs2->empty());
        ASSERT_TRUE(targetIDs2 != nullptr);
        ASSERT_FALSE(targetIDs2->empty());

        const size_t lineCount = df->getRowCount();
        for (size_t i = 0; i < lineCount; i++) {
            returnedLines.add({sourceIDs->at(i),
                               edgeIDs1->at(i), targetIDs1->at(i),
                               edgeIDs2->at(i), targetIDs2->at(i)});
        }
    });

    // Get all expected node IDs
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            const auto edgeView1 = reader.getNodeView(node).edges();
            for (auto edge1 : edgeView1.inEdges()) {
                const auto edgeView2 = reader.getNodeView(edge1._otherID).edges();
                for (auto edge2 : edgeView2.inEdges()) {
                    expectedLines.add({node,
                                       edge1._edgeID, edge1._otherID,
                                       edge2._edgeID, edge2._otherID});
                }
            }
        }
    }

    ASSERT_TRUE(returnedLines.equals(expectedLines));
}

TEST_F(QueriesTest, scanEdges) {
    const std::string query = "MATCH (n)-[e]-(m) RETURN n, e, m";

    LineContainer<NodeID, EdgeID, NodeID> returnedLines;
    LineContainer<NodeID, EdgeID, NodeID> expectedLines;
    _db->query(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 3);
        ASSERT_EQ(df->size(), 3);

        const ColumnNodeIDs* sourceIDs = nullptr;
        const ColumnEdgeIDs* edgeIDs = nullptr;
        const ColumnNodeIDs* targetIDs = nullptr;

        for (auto col : df->cols()) {
            const auto name = col->getName();
            if (name == "n") {
                sourceIDs = col->as<ColumnNodeIDs>();
            } else if (name == "e") {
                edgeIDs = col->as<ColumnEdgeIDs>();
            } else if (name == "m") {
                targetIDs = col->as<ColumnNodeIDs>();
            }
        }

        ASSERT_TRUE(sourceIDs != nullptr);
        ASSERT_FALSE(sourceIDs->empty());
        ASSERT_TRUE(edgeIDs != nullptr);
        ASSERT_FALSE(edgeIDs->empty());
        ASSERT_TRUE(targetIDs != nullptr);
        ASSERT_FALSE(targetIDs->empty());

        const size_t lineCount = targetIDs->size();
        for (size_t i = 0; i < lineCount; i++) {
            returnedLines.add({sourceIDs->at(i), edgeIDs->at(i), targetIDs->at(i)});
        }
    });

    // Get all expected node IDs
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            const auto edgeView = reader.getNodeView(node).edges();
            for (auto edge : edgeView.inEdges()) {
                expectedLines.add({edge._nodeID, edge._edgeID, edge._otherID});
            }
            for (auto edge : edgeView.outEdges()) {
                expectedLines.add({edge._nodeID, edge._edgeID, edge._otherID});
            }
        }
    }

    ASSERT_TRUE(returnedLines.equals(expectedLines));
}

TEST_F(QueriesTest, scanPropertiesWithNull) {
    const std::string query = "MATCH (n)-[e]->(m) RETURN n.age, e.name, m.age";
    using Lines = LineContainer<
        std::optional<int64_t>,
        std::optional<std::optional<std::string_view>>,
        std::optional<int64_t>>;

    auto transaction = _graph->openTransaction();
    auto reader = transaction.readGraph();

    Lines returnedLines;
    Lines expectedLines;

    const auto agePropType = reader.getMetadata().propTypes().get("age");
    const auto namePropType = reader.getMetadata().propTypes().get("name");

    ASSERT_TRUE(agePropType.has_value());
    ASSERT_TRUE(namePropType.has_value());

    _db->query(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 3);
        ASSERT_EQ(df->size(), 3);

        using AgeContainer = ColumnOptVector<types::Int64::Primitive>;
        using NameContainer = ColumnOptVector<types::String::Primitive>;

        const AgeContainer* srcAges = nullptr;
        const NameContainer*  edgeNames = nullptr;
        const AgeContainer* tgtAges = nullptr;

        for (auto* col : df->cols()) {
            const auto name = col->getName();
            if (name == "n.age") {
                srcAges = col->as<AgeContainer>();
            } else if (name == "e.name") {
                edgeNames = col->as<NameContainer>();
            } else if (name == "m.age") {
                tgtAges = col->as<AgeContainer>();
            }
        }


        ASSERT_TRUE(srcAges != nullptr);
        ASSERT_FALSE(srcAges->empty());
        ASSERT_TRUE(edgeNames != nullptr);
        ASSERT_FALSE(edgeNames->empty());
        ASSERT_TRUE(tgtAges != nullptr);
        ASSERT_FALSE(tgtAges->empty());

        const size_t lineCount = tgtAges->size();
        for (size_t i = 0; i < lineCount; i++) {
            returnedLines.add({srcAges->at(i), edgeNames->at(i), tgtAges->at(i)});
        }
    });

    // Get all expected node IDs
    {
        for (const auto& edge: reader.scanOutEdges()) {
            ColumnNodeIDs srcID = {edge._nodeID};
            ColumnEdgeIDs edgeID = {edge._edgeID};
            ColumnNodeIDs tgtID = {edge._otherID};

            std::optional<int64_t> srcAge;
            std::optional<std::optional<std::string_view>> edgeName;
            std::optional<int64_t> tgtAge;

            for (const auto prop : reader.getNodePropertiesWithNull<types::Int64>(agePropType->_id, &srcID)) {
                srcAge = prop;
                break;
            }

            for (const auto prop : reader.getEdgePropertiesWithNull<types::String>(namePropType->_id, &edgeID)) {
                edgeName = prop;
                break;
            }

            for (const auto prop : reader.getNodePropertiesWithNull<types::Int64>(agePropType->_id, &tgtID)) {
                tgtAge = prop;
                break;
            }

            expectedLines.add({srcAge, edgeName, tgtAge});
        }
    }
    fmt::println("- Expected:");
    expectedLines.print(std::cout);
    fmt::println("- Returned:");
    returnedLines.print(std::cout);

    ASSERT_TRUE(returnedLines.equals(expectedLines));
}

TEST_F(QueriesTest, scanNodesCartProd) {
    constexpr std::string_view query = "MATCH (n), (m) RETURN n, m";
    LineContainer<NodeID, NodeID> returnedLines;
    LineContainer<NodeID, NodeID> expectedLines;

    { // Generate ground-truth expected values
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        for (const NodeID n : reader.scanNodes()) {
            for (const NodeID m : reader.scanNodes()) {
                expectedLines.add({n, m});
            }
        }
    }

    {
        _db->query(query, _graphName, &_env->getMem(),
                     [&](const Dataframe* df) -> void {
                         ASSERT_TRUE(df != nullptr);
                         ASSERT_EQ(df->size(), 2);
                         ASSERT_EQ(df->getRowCount(), expectedLines.size());
                         const auto& nCols = df->cols();
                         const auto* n = nCols.front()->as<ColumnNodeIDs>();
                         const auto* m = nCols.back()->as<ColumnNodeIDs>();

                         for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                             returnedLines.add({n->at(rowPtr), m->at(rowPtr)});
                         }
                     });
    }

    ASSERT_TRUE(expectedLines.equals(returnedLines));
}

TEST_F(QueriesTest, getOutSrcXgetOutTgt) {
    constexpr std::string_view query = "MATCH (n)-->(a), (m)-->(b) RETURN n, b";
    using Rows = LineContainer<NodeID, NodeID>;

    Rows expectedRows;

    ColumnNodeIDs ns;
    constexpr std::string_view nQuery = "match (n)-->(a) return n";
    {
        auto res = _db->query(nQuery, _graphName, &_env->getMem(),
                                [&ns](const Dataframe* df) -> void {
                                    ASSERT_TRUE(df);
                                    ASSERT_EQ(1, df->size());
                                    auto* nCol = df->cols().front()->as<ColumnNodeIDs>();
                                    ASSERT_TRUE(nCol);
                                    ns = *nCol;
                                });
        ASSERT_TRUE(res);
    }
    ColumnNodeIDs bs;
    constexpr std::string_view bQuery = "match (m)-->(b) return b";
    {
        auto res = _db->query(bQuery, _graphName, &_env->getMem(),
                                [&bs](const Dataframe* df) -> void {
                                    ASSERT_TRUE(df);
                                    ASSERT_EQ(1, df->size());
                                    auto* bCol = df->cols().front()->as<ColumnNodeIDs>();
                                    ASSERT_TRUE(bCol);
                                    bs = *bCol;
                                });
        ASSERT_TRUE(res);
    }
    for (const NodeID n : ns) {
        for (const NodeID b : bs) {
            expectedRows.add({n, b});
        }
    }

    Rows actualRows;
    {
        QueryStatus res = _db->query(
            query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 2);
                const auto& nCols = df->cols();
                const auto* n = nCols.front()->as<ColumnNodeIDs>();
                ASSERT_TRUE(n);
                const auto* m = nCols.back()->as<ColumnNodeIDs>();
                ASSERT_TRUE(m);

                for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                    actualRows.add({n->at(rowPtr), m->at(rowPtr)});
                }
            });
        ASSERT_TRUE(res);
    }

    EXPECT_TRUE(expectedRows.equals(actualRows));
}

TEST_F(QueriesTest, twoHopXOneHop) {
    constexpr std::string_view query = "MATCH (n)-->(m)-->(o), (p)-->(q) RETURN n, m, o, p, q";
    using Rows = LineContainer<NodeID, NodeID, NodeID, NodeID, NodeID>;

    Rows expectedRows;
    {
        ColumnNodeIDs ns;
        ColumnNodeIDs ms;
        ColumnNodeIDs os;
        constexpr std::string_view nQuery = "match (n)-->(m)-->(o) return n, m, o";
        _db->query(nQuery, _graphName, &_env->getMem(),
                     [&](const Dataframe* df) -> void {
                         ns = *df->cols().at(0)->as<ColumnNodeIDs>();
                         ms = *df->cols().at(1)->as<ColumnNodeIDs>();
                         os = *df->cols().at(2)->as<ColumnNodeIDs>();
                     });

        ColumnNodeIDs ps;
        ColumnNodeIDs qs;
        constexpr std::string_view bQuery = "match (p)-->(q) return p, q";
        _db->query(bQuery, _graphName, &_env->getMem(),
                     [&](const Dataframe* df) -> void {
                         ps = *df->cols().front()->as<ColumnNodeIDs>();
                         qs = *df->cols().back()->as<ColumnNodeIDs>();
                     });

        ASSERT_TRUE((ns.size() == ms.size()) && (ns.size() == os.size()));
        ASSERT_TRUE(ps.size() == qs.size());
        for (size_t i = 0 ; i < ns.size(); i++) {
            for (size_t j = 0; j < ps.size(); j++) {
                expectedRows.add({ns[i], ms[i], os[i], ps[j], qs[j]});
            }
        }
    }

    Rows actualRows;
    {
        QueryStatus res = _db->query(
            query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df != nullptr);
                ASSERT_EQ(df->size(), 5);
                const auto& nCols = df->cols();
                const auto* n = nCols.at(0)->as<ColumnNodeIDs>();
                const auto* m = nCols.at(1)->as<ColumnNodeIDs>();
                const auto* o = nCols.at(2)->as<ColumnNodeIDs>();

                const auto* p = nCols.at(3)->as<ColumnNodeIDs>();
                const auto* q = nCols.at(4)->as<ColumnNodeIDs>();

                for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                    actualRows.add({n->at(rowPtr), m->at(rowPtr), o->at(rowPtr),
                                    p->at(rowPtr), q->at(rowPtr)});
                }
            });
        ASSERT_TRUE(res);
    }

    EXPECT_TRUE(expectedRows.equals(actualRows));
}

TEST_F(QueriesTest, threeCascadingScanNodesCartProd) {
    using Rows = LineContainer<NodeID, NodeID, NodeID>;

    Rows expectedRows;
    ColumnNodeIDs ss;
    ColumnNodeIDs ts;
    ColumnNodeIDs vs;
    {
        constexpr std::string_view scanNodesQuery = "MATCH (n) RETURN n";
        for (auto column : {ss, ts, vs}) {
            auto res = _db->query(scanNodesQuery, _graphName, &_env->getMem(),
                                    [&](const Dataframe* df) -> void {
                                        ASSERT_EQ(df->size(), 1);
                                        column = *df->cols().front()->as<ColumnNodeIDs>();
                                    });
            ASSERT_TRUE(res);
        }
    }
    for (const NodeID s : ss) {
        for (const NodeID t : ts) {
            for (const NodeID v : vs) {
                expectedRows.add({s, t, v});
            }
        }
    }

    Rows actualRows;
    {
        constexpr std::string_view query = "MATCH (s), (t), (v), RETURN s,t,v";
        QueryStatus res = _db->query(
            query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df != nullptr);
                ASSERT_EQ(df->size(), 3);
                const auto& nCols = df->cols();
                const auto* s = nCols.at(0)->as<ColumnNodeIDs>();
                const auto* t = nCols.at(1)->as<ColumnNodeIDs>();
                const auto* v = nCols.at(2)->as<ColumnNodeIDs>();

                for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                    actualRows.add({s->at(rowPtr), t->at(rowPtr), v->at(rowPtr)});
                }
            });
    }

    EXPECT_TRUE(expectedRows.equals(actualRows));
}

TEST_F(QueriesTest, simpleAncestorJoinTest) {
    using Rows = LineContainer<NodeID, NodeID, NodeID>;

    Rows expectedRows;
    ColumnNodeIDs* people = nullptr;
    ColumnNodeIDs* interests = nullptr;
    std::unordered_map<NodeID, std::vector<NodeID>> personToInterestMap;

    {
        constexpr std::string_view scanNodesQuery = "MATCH (n)-->(m) RETURN n,m";
        auto res = _db->query(scanNodesQuery, _graphName, &_env->getMem(),
                              [&](const Dataframe* df) -> void {
                                  ASSERT_EQ(df->size(), 2);
                                  const auto& cols = df->cols();
                                  people = cols[0]->as<ColumnNodeIDs>();
                                  interests = cols[1]->as<ColumnNodeIDs>();
                              });
        ASSERT_TRUE(res);
    }

    for (size_t i = 0; i < people->size(); ++i) {
        personToInterestMap[people->at(i)].emplace_back(interests->at(i));
    }

    for (auto& entry : personToInterestMap) {
        for (auto& it : entry.second) {
            for (auto& it2 : entry.second) {
                expectedRows.add({entry.first, it, it2});
            }
        }
    }


    Rows actualRows;
    {
        constexpr std::string_view query = "MATCH (a)-->(c),(a)-->(b) RETURN a,b,c";
        QueryStatus res = _db->query(
            query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df != nullptr);
                ASSERT_EQ(df->size(), 3);
                const auto& nCols = df->cols();
                const auto* a = nCols.at(0)->as<ColumnNodeIDs>();
                const auto* b = nCols.at(1)->as<ColumnNodeIDs>();
                const auto* c = nCols.at(2)->as<ColumnNodeIDs>();

                for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                    actualRows.add({a->at(rowPtr), b->at(rowPtr), c->at(rowPtr)});
                }
            });
    }

    EXPECT_TRUE(expectedRows.equals(actualRows));
}

TEST_F(QueriesTest, doubleAncestorJoinTest) {
    using Rows = LineContainer<NodeID, NodeID, NodeID, NodeID>;

    Rows expectedRows;
    ColumnNodeIDs* people = nullptr;
    ColumnNodeIDs* interests = nullptr;
    std::unordered_map<NodeID, std::vector<NodeID>> personToInterestMap;

    {
        constexpr std::string_view scanNodesQuery = "MATCH (n)-->(m) RETURN n,m";
        auto res = _db->query(scanNodesQuery, _graphName, &_env->getMem(),
                              [&](const Dataframe* df) -> void {
                                  ASSERT_EQ(df->size(), 2);
                                  const auto& cols = df->cols();
                                  people = cols[0]->as<ColumnNodeIDs>();
                                  interests = cols[1]->as<ColumnNodeIDs>();
                              });
        ASSERT_TRUE(res);
    }

    for (size_t i = 0; i < people->size(); ++i) {
        personToInterestMap[people->at(i)].emplace_back(interests->at(i));
    }

    for (auto& entry : personToInterestMap) {
        for (auto& it : entry.second) {
            for (auto& it2 : entry.second) {
                for (auto& it3 : entry.second) {
                    expectedRows.add({entry.first, it, it2, it3});
                }
            }
        }
    }

    Rows actualRows;
    {
        constexpr std::string_view query = "MATCH (a)-->(c),(a)-->(b), (a)-->(d) RETURN a,b,c,d";
        QueryStatus res = _db->query(
            query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df != nullptr);
                ASSERT_EQ(df->size(), 4);
                const auto& nCols = df->cols();
                const auto* a = nCols.at(0)->as<ColumnNodeIDs>();
                const auto* b = nCols.at(1)->as<ColumnNodeIDs>();
                const auto* c = nCols.at(2)->as<ColumnNodeIDs>();
                const auto* d = nCols.at(3)->as<ColumnNodeIDs>();

                for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                    actualRows.add({a->at(rowPtr), b->at(rowPtr), c->at(rowPtr), d->at(rowPtr)});
                }
            });
    }

    EXPECT_TRUE(expectedRows.equals(actualRows));
}

TEST_F(QueriesTest, simpleSucessorJoinTest) {
    using Rows = LineContainer<NodeID, NodeID, NodeID>;

    Rows expectedRows;
    ColumnNodeIDs* people = nullptr;
    ColumnNodeIDs* interests = nullptr;
    std::unordered_map<NodeID, std::vector<NodeID>> interestToPersonMap;

    {
        constexpr std::string_view scanNodesQuery = "MATCH (n)-->(m) RETURN n,m";
        auto res = _db->query(scanNodesQuery, _graphName, &_env->getMem(),
                              [&](const Dataframe* df) -> void {
                                  ASSERT_EQ(df->size(), 2);
                                  const auto& cols = df->cols();
                                  people = cols[0]->as<ColumnNodeIDs>();
                                  interests = cols[1]->as<ColumnNodeIDs>();
                              });
        ASSERT_TRUE(res);
    }

    for (size_t i = 0; i < people->size(); ++i) {
        interestToPersonMap[interests->at(i)].emplace_back(people->at(i));
    }

    for (auto& entry : interestToPersonMap) {
        for (auto& it : entry.second) {
            for (auto& it2 : entry.second) {
                expectedRows.add({it, it2, entry.first});
            }
        }
    }

    Rows actualRows;
    {
        constexpr std::string_view query = "MATCH (a)-->(c),(b)-->(c) RETURN a,b,c";
        QueryStatus res = _db->query(
            query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df != nullptr);
                ASSERT_EQ(df->size(), 3);
                const auto& nCols = df->cols();
                const auto* a = nCols.at(0)->as<ColumnNodeIDs>();
                const auto* b = nCols.at(1)->as<ColumnNodeIDs>();
                const auto* c = nCols.at(2)->as<ColumnNodeIDs>();

                for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                    actualRows.add({a->at(rowPtr), b->at(rowPtr), c->at(rowPtr)});
                }
            });
    }

    EXPECT_TRUE(expectedRows.equals(actualRows));
}

TEST_F(QueriesTest, doubleSucessorJoinTest) {
    using Rows = LineContainer<NodeID, NodeID, NodeID, NodeID>;

    Rows expectedRows;
    ColumnNodeIDs* people = nullptr;
    ColumnNodeIDs* interests = nullptr;
    std::unordered_map<NodeID, std::vector<NodeID>> interestToPersonMap;

    {
        constexpr std::string_view scanNodesQuery = "MATCH (n)-->(m) RETURN n,m";
        auto res = _db->query(scanNodesQuery, _graphName, &_env->getMem(),
                              [&](const Dataframe* df) -> void {
                                  ASSERT_EQ(df->size(), 2);
                                  const auto& cols = df->cols();
                                  people = cols[0]->as<ColumnNodeIDs>();
                                  interests = cols[1]->as<ColumnNodeIDs>();
                              });
        ASSERT_TRUE(res);
    }

    for (size_t i = 0; i < people->size(); ++i) {
        interestToPersonMap[interests->at(i)].emplace_back(people->at(i));
    }

    for (auto& entry : interestToPersonMap) {
        for (auto& it : entry.second) {
            for (auto& it2 : entry.second) {
                for (auto& it3 : entry.second) {
                    expectedRows.add({it, it2, it3, entry.first});
                }
            }
        }
    }

    Rows actualRows;
    {
        // The insertion order of the join column in the return must match the insertion
        // order of the join column in the expectedRow line container
        constexpr std::string_view query = "MATCH (a)-->(c),(d)-->(c),(b)-->(c) RETURN a,b,d,c";
        QueryStatus res = _db->query(
            query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df != nullptr);
                ASSERT_EQ(df->size(), 4);
                const auto& nCols = df->cols();
                const auto* a = nCols.at(0)->as<ColumnNodeIDs>();
                const auto* b = nCols.at(1)->as<ColumnNodeIDs>();
                const auto* c = nCols.at(2)->as<ColumnNodeIDs>();
                const auto* d = nCols.at(3)->as<ColumnNodeIDs>();

                for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                    actualRows.add({a->at(rowPtr), b->at(rowPtr), c->at(rowPtr), d->at(rowPtr)});
                }
            });
    }

    EXPECT_TRUE(expectedRows.equals(actualRows));
}

TEST_F(QueriesTest, sucessorJoinToExpandEdgeTest) {
    using Rows = LineContainer<NodeID, NodeID, NodeID, NodeID>;

    Rows expectedRows;
    ColumnNodeIDs* col1 = nullptr;
    ColumnNodeIDs* col2 = nullptr;
    std::unordered_map<NodeID, std::vector<NodeID>> col1ToCol2Map;
    std::unordered_map<NodeID, std::vector<NodeID>> col2ToCol1Map;

    {
        constexpr std::string_view scanNodesQuery = "MATCH (n)-->(m) RETURN n,m";
        auto res = _db->query(scanNodesQuery, _graphName, &_env->getMem(),
                              [&](const Dataframe* df) -> void {
                                  ASSERT_EQ(df->size(), 2);
                                  const auto& cols = df->cols();
                                  col1 = cols[0]->as<ColumnNodeIDs>();
                                  col2 = cols[1]->as<ColumnNodeIDs>();
                              });
        ASSERT_TRUE(res);
    }

    for (size_t i = 0; i < col2->size(); ++i) {
        col2ToCol1Map[col2->at(i)].emplace_back(col1->at(i));
        col1ToCol2Map[col1->at(i)].emplace_back(col2->at(i));
    }

    for (auto& entry : col2ToCol1Map) {
        for (auto& it : entry.second) {
            for (auto& it2 : entry.second) {
                for (auto& it3 : col1ToCol2Map[entry.first]) {
                    expectedRows.add({it, it2, entry.first, it3});
                }
            }
        }
    }

    Rows actualRows;
    {
        constexpr std::string_view query = "MATCH (a)-->(c),(b)-->(c)-->(d) RETURN a,b,c,d";
        QueryStatus res = _db->query(
            query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df != nullptr);
                ASSERT_EQ(df->size(), 4);
                const auto& nCols = df->cols();
                const auto* a = nCols.at(0)->as<ColumnNodeIDs>();
                const auto* b = nCols.at(1)->as<ColumnNodeIDs>();
                const auto* c = nCols.at(2)->as<ColumnNodeIDs>();
                const auto* d = nCols.at(3)->as<ColumnNodeIDs>();

                for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                    actualRows.add({a->at(rowPtr), b->at(rowPtr), c->at(rowPtr), d->at(rowPtr)});
                }
            });
    }

    EXPECT_TRUE(expectedRows.equals(actualRows));
}

TEST_F(QueriesTest, xShapedJoinTest) {
    // We only hash on the 3rd column ( the join column)
    struct TupleHash {
        size_t operator()(const std::tuple<NodeID, NodeID, NodeID>& tup) const {
            return std::hash<uint64_t> {}(std::get<2>(tup).getValue());
        }
    };
    using Rows = LineContainer<NodeID, NodeID, NodeID, NodeID, NodeID>;

    Rows expectedRows;
    ColumnNodeIDs* col1;
    ColumnNodeIDs* col2;
    ColumnNodeIDs* col3;
    ColumnNodeIDs* col4;

    // std::unordered_map<std::tuple<NodeID,NodeID,NodeID>,std::vector<NodeID>,TupleHash> tupleToColMap;
    std::unordered_map<NodeID, std::vector<std::tuple<NodeID, NodeID, NodeID>>> colToTupleMap;

    {
        constexpr std::string_view scanNodesQuery = "MATCH (l)-->(m), (n)-->(m)-->(p) RETURN l,n,m,p";
        auto res = _db->query(scanNodesQuery, _graphName, &_env->getMem(),
                                [&](const Dataframe* df) -> void {
                                    ASSERT_TRUE(df != nullptr);
                                    ASSERT_EQ(df->size(), 4);
                                    const auto& nCols = df->cols();
                                    col1 = nCols.at(0)->as<ColumnNodeIDs>();
                                    col2 = nCols.at(1)->as<ColumnNodeIDs>();
                                    col3 = nCols.at(2)->as<ColumnNodeIDs>();
                                    col4 = nCols.at(3)->as<ColumnNodeIDs>();
                                });
        ASSERT_TRUE(res);
    }

    for (size_t i = 0; i < col1->size(); ++i) {
        colToTupleMap[(*col3)[i]].emplace_back(std::make_tuple((*col1)[i], (*col2)[i], (*col4)[i]));
    }

    for (const auto& entry : colToTupleMap) {
        for (auto& it : entry.second) {
            for (auto& it2 : entry.second) {
                auto [col1, col2, joinVal1] = it;
                auto [unused1, unused2, joinVal2] = it2;
                const auto t = std::make_tuple(col1, col2, entry.first, joinVal1, joinVal2);
                expectedRows.add(t);
            }
        }
    }

    Rows actualRows;
    {
        constexpr std::string_view query = "MATCH (a)-->(c)-->(d), (b)-->(c)-->(e) RETURN a,b,c,d,e";
        QueryStatus res = _db->query(
            query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df != nullptr);
                ASSERT_EQ(df->size(), 5);
                const auto& nCols = df->cols();
                const auto* a = nCols.at(0)->as<ColumnNodeIDs>();
                const auto* b = nCols.at(1)->as<ColumnNodeIDs>();
                const auto* c = nCols.at(2)->as<ColumnNodeIDs>();
                const auto* d = nCols.at(3)->as<ColumnNodeIDs>();
                const auto* e = nCols.at(4)->as<ColumnNodeIDs>();

                for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                    actualRows.add({a->at(rowPtr),
                                    b->at(rowPtr),
                                    c->at(rowPtr),
                                    d->at(rowPtr),
                                    e->at(rowPtr)});
                }
            });
    }

    EXPECT_TRUE(expectedRows.equals(actualRows));
}

// The type of join for this test has not been implemented yet - so it is disabled for now
TEST_F(QueriesTest, blockedBinaryQuery) {
    constexpr std::string_view query = "MATCH (n)-[e]->(m), (n)<-[f]-(m) return n, e, m, f";
    QueryStatus res = _db->query(query, _graphName, &_env->getMem(),
                                   [&](const Dataframe* df) -> void {});
    ASSERT_FALSE(res);
    ASSERT_TRUE(res.hasErrorMessage());
    EXPECT_EQ(res.getError(), std::string("Undirected Join Path On Common Successor Not Supported"));
}

TEST_F(QueriesTest, db_labels) {
    using Rows = LineContainer<LabelID, std::string_view>;
    auto transaction = _graph->openTransaction();
    auto reader = transaction.readGraph();

    Rows expectedRows;
    Rows actualRows;

    const auto& metadata = reader.getMetadata();
    const auto& labels = metadata.labels();

    for (const auto& [id, name] : labels) {
        expectedRows.add({id, *name});
    }

    _db->query("CALL db.labels()", _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), expectedRows.lineSize());
        ASSERT_EQ(df->getRowCount(), expectedRows.size());
        const auto& cols = df->cols();
        const auto* col1 = cols.at(0)->as<ColumnVector<LabelID>>();
        const auto* col2 = cols.at(1)->as<ColumnVector<std::string_view>>();

        for (size_t i = 0; i < df->getRowCount(); i++) {
            actualRows.add({col1->at(i), col2->at(i)});
        }
    });

    EXPECT_TRUE(expectedRows.equals(actualRows));
    actualRows.clear();

    _db->query("CALL db.labels() YIELD id, label",
                 _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                     ASSERT_TRUE(df != nullptr);
                     ASSERT_EQ(df->cols().size(), 2);
                     ASSERT_EQ(df->getRowCount(), expectedRows.size());
                     const auto& cols = df->cols();
                     const auto* col1 = cols.at(0)->as<ColumnVector<LabelID>>();
                     const auto* col2 = cols.at(1)->as<ColumnVector<std::string_view>>();

                     for (size_t i = 0; i < df->getRowCount(); i++) {
                         actualRows.add({col1->at(i), col2->at(i)});
                     }
                 });

    EXPECT_TRUE(expectedRows.equals(actualRows));

    actualRows.clear();
}

TEST_F(QueriesTest, db_edgeTypes) {
    using Rows = LineContainer<EdgeTypeID, std::string_view>;
    auto transaction = _graph->openTransaction();
    auto reader = transaction.readGraph();

    Rows expectedRows;
    Rows actualRows;

    const auto& metadata = reader.getMetadata();
    const auto& edgeTypes = metadata.edgeTypes();

    for (const auto& [id, name] : edgeTypes) {
        expectedRows.add({id, *name});
    }

    _db->query("CALL db.edgeTypes()", _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), expectedRows.lineSize());
        ASSERT_EQ(df->getRowCount(), expectedRows.size());
        const auto& cols = df->cols();
        const auto* col1 = cols.at(0)->as<ColumnVector<EdgeTypeID>>();
        const auto* col2 = cols.at(1)->as<ColumnVector<std::string_view>>();

        for (size_t i = 0; i < df->getRowCount(); i++) {
            actualRows.add({col1->at(i), col2->at(i)});
        }
    });

    EXPECT_TRUE(expectedRows.equals(actualRows));
    actualRows.clear();

    _db->query("CALL db.edgeTypes() YIELD id, edgeType",
                 _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                     ASSERT_TRUE(df != nullptr);
                     ASSERT_EQ(df->cols().size(), 2);
                     ASSERT_EQ(df->getRowCount(), expectedRows.size());
                     const auto& cols = df->cols();
                     const auto* col1 = cols.at(0)->as<ColumnVector<EdgeTypeID>>();
                     const auto* col2 = cols.at(1)->as<ColumnVector<std::string_view>>();

                     for (size_t i = 0; i < df->getRowCount(); i++) {
                         actualRows.add({col1->at(i), col2->at(i)});
                     }
                 });

    EXPECT_TRUE(expectedRows.equals(actualRows));

    actualRows.clear();
}

TEST_F(QueriesTest, db_propertyTypes) {
    using Rows = LineContainer<PropertyTypeID, std::string_view, ValueType>;
    auto transaction = _graph->openTransaction();
    auto reader = transaction.readGraph();

    Rows expectedRows;
    Rows actualRows;

    const auto& metadata = reader.getMetadata();
    const auto& propTypes = metadata.propTypes();

    for (const auto& [pt, name] : propTypes) {
        expectedRows.add({pt._id, *name, pt._valueType});
    }

    _db->query("CALL db.propertyTypes()", _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), expectedRows.lineSize());
        ASSERT_EQ(df->getRowCount(), expectedRows.size());
        const auto& cols = df->cols();
        const auto* col1 = cols.at(0)->as<ColumnVector<PropertyTypeID>>();
        const auto* col2 = cols.at(1)->as<ColumnVector<std::string_view>>();
        const auto* col3 = cols.at(2)->as<ColumnVector<ValueType>>();

        for (size_t i = 0; i < df->getRowCount(); i++) {
            actualRows.add({col1->at(i), col2->at(i), col3->at(i)});
        }
    });

    EXPECT_TRUE(expectedRows.equals(actualRows));
    actualRows.clear();

    _db->query("CALL db.propertyTypes() YIELD id, propertyType, valueType",
                 _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                     ASSERT_TRUE(df != nullptr);
                     ASSERT_EQ(df->cols().size(), 3);
                     ASSERT_EQ(df->getRowCount(), expectedRows.size());
                     const auto& cols = df->cols();
                     const auto* col1 = cols.at(0)->as<ColumnVector<PropertyTypeID>>();
                     const auto* col2 = cols.at(1)->as<ColumnVector<std::string_view>>();
                     const auto* col3 = cols.at(2)->as<ColumnVector<ValueType>>();

                     for (size_t i = 0; i < df->getRowCount(); i++) {
                         actualRows.add({col1->at(i), col2->at(i), col3->at(i)});
                     }
                 });

    EXPECT_TRUE(expectedRows.equals(actualRows));

    actualRows.clear();
}

TEST_F(QueriesTest, db_history) {
    using Rows = LineContainer<std::string, uint64_t, uint64_t, uint64_t>;
    auto transaction = _graph->openTransaction();
    auto reader = transaction.readGraph();

    Rows expectedRows;
    Rows actualRows;

    const std::span commits = reader.commits();

    for (const auto& commit : commits) {
        const CommitHash& hash = commit.hash();
        const std::span parts = commit.dataparts();
        const Tombstones& tombstones = commit.tombstones();

        size_t nodeCount = 0;
        for (const auto& part : parts) {
            nodeCount += part->getNodeContainerSize();
        }
        nodeCount -= tombstones.numNodes();

        size_t edgeCount = 0;
        for (const auto& part : parts) {
            edgeCount += part->getEdgeContainerSize();
        }
        edgeCount -= tombstones.numEdges();

        expectedRows.add({fmt::format("{:x}", hash.get()),
                          nodeCount,
                          edgeCount,
                          parts.size()});
    }

    _db->query("CALL db.history()", _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), expectedRows.lineSize());
        ASSERT_EQ(df->getRowCount(), expectedRows.size());
        const auto& cols = df->cols();
        const auto* col1 = cols.at(0)->as<ColumnVector<std::string>>();
        const auto* col2 = cols.at(1)->as<ColumnVector<uint64_t>>();
        const auto* col3 = cols.at(2)->as<ColumnVector<uint64_t>>();
        const auto* col4 = cols.at(3)->as<ColumnVector<uint64_t>>();

        for (size_t i = 0; i < df->getRowCount(); i++) {
            actualRows.add({col1->at(i), col2->at(i), col3->at(i), col4->at(i)});
        }
    });

    EXPECT_TRUE(expectedRows.equals(actualRows));
    actualRows.clear();

    _db->query("CALL db.history() YIELD commit, nodeCount, edgeCount, partCount",
                 _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                     ASSERT_TRUE(df != nullptr);
                     ASSERT_EQ(df->cols().size(), 4);
                     ASSERT_EQ(df->getRowCount(), expectedRows.size());
                     const auto& cols = df->cols();
                     const auto* col1 = cols.at(0)->as<ColumnVector<std::string>>();
                     const auto* col2 = cols.at(1)->as<ColumnVector<uint64_t>>();
                     const auto* col3 = cols.at(2)->as<ColumnVector<uint64_t>>();
                     const auto* col4 = cols.at(3)->as<ColumnVector<uint64_t>>();

                     for (size_t i = 0; i < df->getRowCount(); i++) {
                         actualRows.add({col1->at(i), col2->at(i), col3->at(i), col4->at(i)});
                     }
                 });

    EXPECT_TRUE(expectedRows.equals(actualRows));

    actualRows.clear();
}

TEST_F(QueriesTest, scanByLabelOutEdges) {
    const std::string query = "MATCH (n:Person)-[e]->(m) RETURN n, e, m";

    LineContainer<NodeID, EdgeID, NodeID> returnedLines;
    LineContainer<NodeID, EdgeID, NodeID> expectedLines;
    _db->query(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 3);
        ASSERT_EQ(df->size(), 3);

        const ColumnNodeIDs* sourceIDs = nullptr;
        const ColumnEdgeIDs* edgeIDs = nullptr;
        const ColumnNodeIDs* targetIDs = nullptr;

        for (auto col : df->cols()) {
            const auto name = col->getName();
            if (name == "n") {
                sourceIDs = col->as<ColumnNodeIDs>();
            } else if (name == "e") {
                edgeIDs = col->as<ColumnEdgeIDs>();
            } else if (name == "m") {
                targetIDs = col->as<ColumnNodeIDs>();
            }
        }

        ASSERT_TRUE(sourceIDs != nullptr);
        ASSERT_FALSE(sourceIDs->empty());
        ASSERT_TRUE(edgeIDs != nullptr);
        ASSERT_FALSE(edgeIDs->empty());
        ASSERT_TRUE(targetIDs != nullptr);
        ASSERT_FALSE(targetIDs->empty());

        const size_t lineCount = targetIDs->size();
        for (size_t i = 0; i < lineCount; i++) {
            returnedLines.add({sourceIDs->at(i), edgeIDs->at(i), targetIDs->at(i)});
        }
    });

    // Get all expected node IDs from :Person
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();

        const LabelID labelPerson = reader.getMetadata().labels().get("Person").value();

        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            const LabelSetHandle nodeLabelSet = reader.getNodeLabelSet(node);
            if (!nodeLabelSet.hasLabel(labelPerson)) {
                continue;
            }

            const auto edgeView = reader.getNodeView(node).edges();
            for (auto edge : edgeView.outEdges()) {
                expectedLines.add({edge._nodeID, edge._edgeID, edge._otherID});
            }
        }
    }

    ASSERT_TRUE(returnedLines.equals(expectedLines));
}

TEST_F(QueriesTest, scanNodesByLabel) {
    const std::string query = "MATCH (n:Person) RETURN n";

    LineContainer<NodeID> returnedLines;
    LineContainer<NodeID> expectedLines;
    _db->query(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 1);

        ColumnNodeIDs* nodeIDs = df->cols().front()->as<ColumnNodeIDs>();

        const size_t lineCount = nodeIDs->size();
        for (size_t i = 0; i < lineCount; i++) {
            returnedLines.add({nodeIDs->at(i)});
        }
    });

    // Get all expected node IDs from :Person
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();

        const LabelID labelPerson = reader.getMetadata().labels().get("Person").value();

        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            const LabelSetHandle nodeLabelSet = reader.getNodeLabelSet(node);
            if (!nodeLabelSet.hasLabel(labelPerson)) {
                continue;
            }
            expectedLines.add({node});
        }
    }

    ASSERT_TRUE(returnedLines.equals(expectedLines));
}

TEST_F(QueriesTest, change) {
    {
        const std::string query = "CHANGE NEW";
        _db->query(query, _graphName, &_env->getMem(), [](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->size(), 1);
            ASSERT_EQ(df->cols().size(), 1);

            const ColumnVector<ChangeID>* changeIDs = df->cols().front()->as<ColumnVector<ChangeID>>();
            ASSERT_TRUE(changeIDs != nullptr);
            ASSERT_FALSE(changeIDs->empty());
            ASSERT_EQ(changeIDs->size(), 1);

            const ChangeID changeID = changeIDs->at(0);
            EXPECT_EQ(changeID, ChangeID {4});
        });
    }

    {
        const std::string query = "CHANGE LIST";
        _db->query(query, _graphName, &_env->getMem(), [](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->size(), 1);
            ASSERT_EQ(df->cols().size(), 1);

            const ColumnVector<ChangeID>* changeIDs = df->cols().front()->as<ColumnVector<ChangeID>>();
            ASSERT_TRUE(changeIDs != nullptr);
            ASSERT_FALSE(changeIDs->empty());
            ASSERT_EQ(changeIDs->size(), 1);

            const ChangeID changeID = changeIDs->at(0);
            EXPECT_EQ(changeID, ChangeID {4});
        });
    }

    {
        const std::string query = "CHANGE SUBMIT";
        _db->query(query, _graphName, &_env->getMem(), [](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->size(), 1);
            ASSERT_EQ(df->cols().size(), 1);

            const ColumnVector<ChangeID>* changeIDs = df->cols().front()->as<ColumnVector<ChangeID>>();
            ASSERT_TRUE(changeIDs != nullptr);
            ASSERT_FALSE(changeIDs->empty());
            ASSERT_EQ(changeIDs->size(), 1);

            const ChangeID changeID = changeIDs->at(0);
            EXPECT_EQ(changeID, ChangeID {4});
        }, CommitHash::head(), ChangeID {4});
    }

    {
        const std::string query = "CHANGE LIST";
        _db->query(query, _graphName, &_env->getMem(), [](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->size(), 1);
            ASSERT_EQ(df->cols().size(), 1);

            const ColumnVector<ChangeID>* changeIDs = df->cols().front()->as<ColumnVector<ChangeID>>();
            ASSERT_TRUE(changeIDs != nullptr);
            ASSERT_TRUE(changeIDs->empty());
            ASSERT_EQ(changeIDs->size(), 0);
        });
    }

    {
        const std::string query = "CHANGE NEW";
        _db->query(query, _graphName, &_env->getMem(), [](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->size(), 1);
            ASSERT_EQ(df->cols().size(), 1);

            const ColumnVector<ChangeID>* changeIDs = df->cols().front()->as<ColumnVector<ChangeID>>();
            ASSERT_TRUE(changeIDs != nullptr);
            ASSERT_FALSE(changeIDs->empty());
            ASSERT_EQ(changeIDs->size(), 1);

            const ChangeID changeID = changeIDs->at(0);
            EXPECT_EQ(changeID, ChangeID {5});
        });
    }

    {
        const std::string query = "CHANGE DELETE";
        _db->query(query, _graphName, &_env->getMem(), [](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->size(), 1);
            ASSERT_EQ(df->cols().size(), 1);

            const ColumnVector<ChangeID>* changeIDs = df->cols().front()->as<ColumnVector<ChangeID>>();
            ASSERT_TRUE(changeIDs != nullptr);
            ASSERT_FALSE(changeIDs->empty());
            ASSERT_EQ(changeIDs->size(), 1);

            const ChangeID changeID = changeIDs->at(0);
            EXPECT_EQ(changeID, ChangeID {5});
        }, CommitHash::head(), ChangeID {5});
    }

    {
        const std::string query = "CHANGE LIST";
        _db->query(query, _graphName, &_env->getMem(), [](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->size(), 1);
            ASSERT_EQ(df->cols().size(), 1);

            const ColumnVector<ChangeID>* changeIDs = df->cols().front()->as<ColumnVector<ChangeID>>();
            ASSERT_TRUE(changeIDs != nullptr);
            ASSERT_TRUE(changeIDs->empty());
            ASSERT_EQ(changeIDs->size(), 0);
        });
    }
}

TEST_F(QueriesTest, db_listGraph) {
    ColumnVector<std::string_view> expectedGraphNames;
    ColumnVector<std::string_view> actualGraphNames;
    _env->getSystemManager().listGraphs(expectedGraphNames.getRaw());

    bool callBackExecuted = false;
    _db->query("list graph", _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        callBackExecuted = true;
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 1);
        ASSERT_EQ(df->getRowCount(), expectedGraphNames.size());
        const auto& cols = df->cols();
        auto* actualGraphNames = cols.at(0)->as<ColumnVector<std::string_view>>();

        EXPECT_EQ(expectedGraphNames.getRaw(), actualGraphNames->getRaw());
    });

    ASSERT_TRUE(callBackExecuted);
}

TEST_F(QueriesTest, db_commit) {
    auto transaction = _graph->openTransaction();
    auto reader = transaction.readGraph();

    const auto originalCommitHash = reader.commits().back().hash();
    CommitHash newCommitHash;
    ChangeID changeID;

    {
        const std::string query = "COMMIT";
        auto res = _db->query(query, _graphName, &_env->getMem(), [](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->size(), 0);
            ASSERT_EQ(df->cols().size(), 0);
        });

        EXPECT_TRUE(res.hasErrorMessage());
        EXPECT_EQ(res.getError(), "CommitProcessor: Cannot commit outside of a write transaction");
    }

    {
        const std::string query = "CHANGE NEW";
        _db->query(query, _graphName, &_env->getMem(), [&changeID](const Dataframe* df) -> void {
            const ColumnVector<ChangeID>* changeIDs = df->cols().front()->as<ColumnVector<ChangeID>>();

            changeID = changeIDs->at(0);
        });
        auto transaction = _env->getSystemManager().openTransaction(_graphName,
                                                                    CommitHash::head(),
                                                                    changeID);
        auto reader = transaction->readGraph();
        newCommitHash = reader.commits().back().hash();
        EXPECT_NE(originalCommitHash, newCommitHash);
    }

    {
        const std::string query = "COMMIT";
        auto res = _db->query(query, _graphName, &_env->getMem(), [](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->size(), 0);
            ASSERT_EQ(df->cols().size(), 0); }, CommitHash::head(), changeID);

        auto transaction = _env->getSystemManager().openTransaction(_graphName,
                                                                    CommitHash::head(),
                                                                    changeID);
        auto reader = transaction->readGraph();
        const auto finalCommitHash = reader.commits().back().hash();

        EXPECT_FALSE(res.hasErrorMessage());
        EXPECT_NE(newCommitHash, finalCommitHash);
        EXPECT_NE(originalCommitHash, finalCommitHash);
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}

TEST_F(QueriesTest, whereName) {
    const auto MATCH_QUERY = [](std::string_view name) -> std::string {
        return fmt::format("MATCH (n) WHERE n.name = \"{}\" RETURN n", name);
    };

    using Rows = LineContainer<NodeID, std::string_view>;

    std::vector<std::string_view> names;
    Rows expected;
    {
        // TODO: Find a way to dynamically get the PropertyID of the "name" property
        const PropertyTypeID nameID = 0;
        NodeID nextNodeID = 0;
        for (std::string_view name : read().scanNodeProperties<types::String>(nameID)) {
            expected.add({nextNodeID++, name});
            names.emplace_back(name);
        }
    }

    Rows actual;
    {
        for (std::string_view name : names) {
            auto res = query(MATCH_QUERY(name), [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(1, df->size()); // Just the 'n' column
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                ASSERT_EQ(1, ns->size());
                NodeID n = ns->front();
                actual.add({n, name});
            });
            ASSERT_TRUE(res);
        }
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(QueriesTest, predicateOR) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.hasPhD OR n.isFrench RETURN n";
    // TODO: Find way to get these PropertyIDs dynamically
    const PropertyTypeID ISFRENCH_PROPID = 3;
    const PropertyTypeID HASPHD_PROPID = 4;

    using Boolean = types::Bool::Primitive;
    using Rows = LineContainer<NodeID>;

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const Boolean* french = read().tryGetNodeProperty<types::Bool>(ISFRENCH_PROPID, n);
            const Boolean* phd = read().tryGetNodeProperty<types::Bool>(HASPHD_PROPID, n);
            if ((french && *french) || (phd && *phd)) {
                expected.add({n});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(1, df->size()); // Just the 'n' column
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(QueriesTest, predicateAND) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.hasPhD AND n.isFrench RETURN n";
    // TODO: Find way to get these PropertyIDs dynamically
    const PropertyTypeID ISFRENCH_PROPID = 3;
    const PropertyTypeID HASPHD_PROPID = 4;

    using Boolean = types::Bool::Primitive;
    using Rows = LineContainer<NodeID>;

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const Boolean* french = read().tryGetNodeProperty<types::Bool>(ISFRENCH_PROPID, n);
            const Boolean* phd = read().tryGetNodeProperty<types::Bool>(HASPHD_PROPID, n);
            if (!french || !phd) {
                continue;
            }
            if (*french && *phd) {
                expected.add({n});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(1, df->size()); // Just the 'n' column
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(QueriesTest, predicateNOT) {
    std::string_view MATCH_QUERY = "MATCH (n) WHERE NOT n.isFrench RETURN n, n.name";
    // TODO: Find way to get these PropertyIDs dynamically
    const PropertyTypeID NAME_PROPID = 0;
    const PropertyTypeID ISFRENCH_PROPID = 3;

    using Boolean = types::Bool::Primitive;
    using String = types::String::Primitive;
    using Rows = LineContainer<NodeID, String>;

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const Boolean* french = read().tryGetNodeProperty<types::Bool>(ISFRENCH_PROPID, n);
            if (french != nullptr && !*french) {
                const String* name = read().tryGetNodeProperty<types::String>(NAME_PROPID, n);
                expected.add({n, *name});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(2, df->size());
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            auto* names = df->cols().back()->as<ColumnVector<std::optional<String>>>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(names);
            for (size_t row = 0; row < ns->size(); row++) {
                auto& n = ns->at(row);
                auto& name = names->at(row);
                actual.add({n, *name});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(QueriesTest, cartProdThenFilter) {
    // Match pairs of (French, Not French)
    std::string_view MATCH_QUERY = "MATCH (n), (m) WHERE n.isFrench AND NOT m.isFrench RETURN n.name, m.name";
    const PropertyTypeID NAME_PROPID = 0;
    const PropertyTypeID ISFRENCH_PROPID = 3;

    using Boolean = types::Bool::Primitive;
    using String = types::String::Primitive;
    using Rows = LineContainer<String, String>;

    Rows expected;
    {
        for (NodeID n : read().scanNodes()) {
            const Boolean* nfrench = read().tryGetNodeProperty<types::Bool>(ISFRENCH_PROPID, n);
            if (!nfrench || !*nfrench) {
                continue;
            }
            const String* nname = read().tryGetNodeProperty<types::String>(NAME_PROPID, n);
            for (NodeID m : read().scanNodes()) {
                const Boolean* mfrench = read().tryGetNodeProperty<types::Bool>(ISFRENCH_PROPID, m);
                if (!mfrench || *mfrench) {
                    continue;
                }
                const String* mname = read().tryGetNodeProperty<types::String>(NAME_PROPID, m);
                expected.add({*nname, *mname});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(2, df->size());
            auto* nnames = df->cols().front()->as<ColumnVector<std::optional<String>>>();
            auto* mnames = df->cols().back()->as<ColumnVector<std::optional<String>>>();
            ASSERT_TRUE(nnames);
            ASSERT_TRUE(mnames);
            for (size_t row = 0; row < nnames->size(); row++) {
                auto& nname = nnames->at(row);
                auto& mname = mnames->at(row);
                actual.add({*nname, *mname});
            }
        });
        ASSERT_TRUE(res);
    }

    ASSERT_TRUE(expected.equals(actual));
}

TEST_F(QueriesTest, complexPredicate) {
    // NOTE: Equivalent to (NOT isFrench OR hasPhD)
    std::string_view MATCH_QUERY = "MATCH (n) WHERE NOT (n.isFrench AND NOT n.hasPhD) RETURN n, n.name";
    const PropertyTypeID NAME_PROPID = 0;
    const PropertyTypeID ISFRENCH_PROPID = 3;
    const PropertyTypeID HASPHD_PROPID = 4;

    using Boolean = types::Bool::Primitive;
    using String = types::String::Primitive;
    using Rows = LineContainer<NodeID, String>;

    Rows expected;
    {
        for (NodeID n : read().scanNodes()) {
            const Boolean* french = read().tryGetNodeProperty<types::Bool>(ISFRENCH_PROPID, n);
            const Boolean* phd = read().tryGetNodeProperty<types::Bool>(HASPHD_PROPID, n);
            if (!phd || !french) { // Don't have these props: skip
                continue;
            }
            if (!*french || *phd) { // not french or has phd
                const String* name =
                    read().tryGetNodeProperty<types::String>(NAME_PROPID, n);
                expected.add({n, *name});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(2, df->size());
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            auto* names = df->cols().back()->as<ColumnVector<std::optional<String>>>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(names);
            ASSERT_EQ(ns->size(), names->size());
            for (size_t row = 0; row < names->size(); row++) {
                NodeID n = ns->at(row);
                auto& name = names->at(row);
                actual.add({n, *name});
            }
        });
        ASSERT_TRUE(res);
    }

    ASSERT_TRUE(expected.equals(actual));
}

TEST_F(QueriesTest, emptyResultProjectDifferentProp) {
    std::string_view MATCH_QUERY = "MATCH (n) WHERE n.isFrench AND NOT n.isFrench RETURN n.name";

    using String = types::String::Primitive;
    using Rows = LineContainer<String>;

    Rows expected; // empty

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(1, df->size());
            auto* names = df->cols().front()->as<ColumnVector<std::optional<String>>>();
            ASSERT_TRUE(names);
            EXPECT_EQ(0, names->size());
            for (auto& name : *names) {
                actual.add({*name});
            }
        });
        ASSERT_TRUE(res);
    }

    ASSERT_TRUE(expected.equals(actual));
}

TEST_F(QueriesTest, edgeTypeFilter) {
    const EdgeTypeID INTERESTED_IN_TYPEID = 1; // TODO find dynamically
    std::string_view MATCH_QUERY = "MATCH (n)-[e:INTERESTED_IN]->(m) RETURN e";

    using Rows = LineContainer<EdgeID>;

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getEdgeTypeID(e._edgeID) == INTERESTED_IN_TYPEID) {
                expected.add({e._edgeID});
            }
        }
    }

    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(1, df->size());
            auto* es = df->cols().front()->as<ColumnEdgeIDs>();
            ASSERT_TRUE(es);
            for (EdgeID e : *es) {
                actual.add({e});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(QueriesTest, pitchDeckPersonInterest) {
    const std::string_view MATCH_QUERY = R"(MATCH (p:Person)-->(i:Interest) RETURN p, i)";

    const auto personLabelOpt = read().getView().metadata().labels().get("Person");
    ASSERT_TRUE(personLabelOpt);
    const LabelID personLabel = *personLabelOpt;

    const auto interestLabelOpt = read().getView().metadata().labels().get("Interest");
    ASSERT_TRUE(interestLabelOpt);
    const LabelID interestLabel = *interestLabelOpt;

    using Rows = LineContainer<NodeID, NodeID>;

    Rows expected;
    {
        ColumnNodeIDs people;
        for (NodeID n : read().scanNodes()) {
            NodeView v = read().getNodeView(n);
            if (v.labelset().hasLabel(personLabel)) {
                people.push_back(n);
            }
        }
        for (EdgeRecord e : read().getOutEdges(&people)) {
            NodeView v = read().getNodeView(e._otherID);
            if (v.labelset().hasLabel(interestLabel)) {
                expected.add({e._nodeID, e._otherID});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(2, df->size());
            auto* ps = df->cols().front()->as<ColumnNodeIDs>();
            auto* is = df->cols().back()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ps);
            ASSERT_TRUE(is);
            ASSERT_EQ(ps->size(), is->size());
            for (size_t i = 0; i < ps->size(); i++) {
                actual.add({ps->at(i), is->at(i)});
            }
        });
        ASSERT_TRUE(res);
    }
    ASSERT_TRUE(expected.equals(actual));
}

TEST_F(QueriesTest, int64FilterOperands) {
    using Duration = types::Int64::Primitive;
    using Rows = LineContainer<EdgeID, Duration>;

    // Set up
    auto testOperand = [this](std::string_view matchQuery,
                              Duration threshold,
                              auto comp) -> void {
        PropertyTypeID durationProp = getPropID("duration");

        Rows expected;
        {
            for (const EdgeRecord& e : read().scanOutEdges()) {

                const auto* duration =
                    read().tryGetEdgeProperty<types::Int64>(durationProp, e._edgeID);
                if (duration && comp(*duration, threshold)) {
                    expected.add({e._edgeID, *duration});
                }
            }
        }

        Rows actual;
        {
            auto res = query(matchQuery, [&actual](const Dataframe* df) -> void {
                ASSERT_TRUE(df);

                auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
                auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Duration>>();

                ASSERT_TRUE(es);
                ASSERT_TRUE(durs);
                ASSERT_EQ(es->size(), durs->size());

                for (size_t row = 0; row < es->size(); row++) {
                    actual.add({es->at(row), *durs->at(row)});
                }
            });

            ASSERT_TRUE(res);
        }

        EXPECT_TRUE(expected.equals(actual));
    };

    // Test cases
    {
        std::string_view matchQuery = R"(MATCH (n)-[e]->(m) WHERE e.duration > 15 RETURN e, e.duration)";
        Duration threshold = 15;
        testOperand(matchQuery, threshold, std::greater<Duration> {});
    }

    /*
    {
        std::string_view matchQuery = R"(MATCH (n)-[e]->(m) WHERE e.duration < 15 RETURN e)";
        Duration threshold = 15;
        testOperand(matchQuery, threshold, std::less<Duration> {});
    }

    {
        std::string_view matchQuery = R"(MATCH (n)-[e]->(m) WHERE e.duration >= 15 RETURN e)";
        Duration threshold = 15;
        testOperand(matchQuery, threshold, std::greater_equal<Duration> {});
    }

    {
        std::string_view matchQuery = R"(MATCH (n)-[e]->(m) WHERE e.duration <= 15 RETURN e)";
        Duration threshold = 15;
        testOperand(matchQuery, threshold, std::less_equal<Duration> {});
    }
    */
}
