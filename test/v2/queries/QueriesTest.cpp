#include <gtest/gtest.h>

#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/Block.h"
#include "columns/ColumnIDs.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"

#include "LineContainer.h"
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
    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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
        _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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
    const auto result = _db->queryV2(query, _graphName, &_env->getMem(), [](const Dataframe* df) -> void {});
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
        _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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
            _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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
    const std::string query = "MATCH (n)-->(m) RETURN n, m";

    LineContainer<NodeID,NodeID> returned;
    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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

        ASSERT_EQ(targetIDs->size(), sourceIDs->size());
        const size_t size = targetIDs->size();
        for (size_t i = 0; i < size; i++) {
            returned.add({sourceIDs->at(i), targetIDs->at(i)});
        }
    });

    // Get all expected node IDs
    LineContainer<NodeID, NodeID> expected;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            const auto edgeView = reader.getNodeView(node).edges();
            for (auto edge : edgeView.outEdges()) {
                expected.add({node, edge._otherID});
            }
        }
    }

    ASSERT_TRUE(returned.equals(expected));
}

TEST_F(QueriesTest, scanExpand2) {
    const std::string query = "MATCH (n)-->(m)-->(t) RETURN n, m, t";

    std::vector<NodeID> returnedTargets1;
    std::vector<NodeID> returnedTargets2;
    std::vector<NodeID> returnedSources;
    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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
    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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
    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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
    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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

    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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
        _db->queryV2(query, _graphName, &_env->getMem(),
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
        auto res = _db->queryV2(nQuery, _graphName, &_env->getMem(),
                                [&](const Dataframe* df) -> void {
                                    ns = *df->cols().front()->as<ColumnNodeIDs>();
                                });
        ASSERT_TRUE(res);
    }
    ColumnNodeIDs bs;
    constexpr std::string_view bQuery = "match (m)-->(b) return b";
    {
        auto res = _db->queryV2(bQuery, _graphName, &_env->getMem(),
                                [&](const Dataframe* df) -> void {
                                    bs = *df->cols().front()->as<ColumnNodeIDs>();
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
        QueryStatus res = _db->queryV2(
            query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df != nullptr);
                ASSERT_EQ(df->size(), 2);
                const auto& nCols = df->cols();
                const auto* n = nCols.front()->as<ColumnNodeIDs>();
                const auto* b = nCols.back()->as<ColumnNodeIDs>();

                for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                    actualRows.add({n->at(rowPtr), b->at(rowPtr)});
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
        _db->queryV2(nQuery, _graphName, &_env->getMem(),
                     [&](const Dataframe* df) -> void {
                         ns = *df->cols().at(0)->as<ColumnNodeIDs>();
                         ms = *df->cols().at(1)->as<ColumnNodeIDs>();
                         os = *df->cols().at(2)->as<ColumnNodeIDs>();
                     });

        ColumnNodeIDs ps;
        ColumnNodeIDs qs;
        constexpr std::string_view bQuery = "match (p)-->(q) return p, q";
        _db->queryV2(bQuery, _graphName, &_env->getMem(),
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
        QueryStatus res = _db->queryV2(
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
            auto res = _db->queryV2(scanNodesQuery, _graphName, &_env->getMem(),
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
        QueryStatus res = _db->queryV2(
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
    ColumnNodeIDs people;
    ColumnNodeIDs interests;
    std::unordered_map<NodeID, std::vector<NodeID>> personToInterestMap;

    {
        constexpr std::string_view scanNodesQuery = "MATCH (n)--(m) RETURN n,m";
        auto res = _db->query(scanNodesQuery, _graphName, &_env->getMem(),
                              [&](const Block& block) -> void {
                                  ASSERT_EQ(block.size(), 2);
                                  people = *static_cast<ColumnNodeIDs*>(block.columns()[0]);
                                  interests = *static_cast<ColumnNodeIDs*>(block.columns()[1]);
                              });
        ASSERT_TRUE(res);
    }

    for (size_t i = 0; i < people.size(); ++i) {
        personToInterestMap[people[i]].emplace_back(interests[i]);
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
        QueryStatus res = _db->queryV2(
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
    ColumnNodeIDs people;
    ColumnNodeIDs interests;
    std::unordered_map<NodeID, std::vector<NodeID>> personToInterestMap;

    {
        constexpr std::string_view scanNodesQuery = "MATCH (n)--(m) RETURN n,m";
        auto res = _db->query(scanNodesQuery, _graphName, &_env->getMem(),
                              [&](const Block& block) -> void {
                                  ASSERT_EQ(block.size(), 2);
                                  people = *static_cast<ColumnNodeIDs*>(block.columns()[0]);
                                  interests = *static_cast<ColumnNodeIDs*>(block.columns()[1]);
                              });
        ASSERT_TRUE(res);
    }

    for (size_t i = 0; i < people.size(); ++i) {
        personToInterestMap[people[i]].emplace_back(interests[i]);
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
        QueryStatus res = _db->queryV2(
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
    ColumnNodeIDs people;
    ColumnNodeIDs interests;
    std::unordered_map<NodeID, std::vector<NodeID>> interestToPersonMap;

    {
        constexpr std::string_view scanNodesQuery = "MATCH (n)--(m) RETURN n,m";
        auto res = _db->query(scanNodesQuery, _graphName, &_env->getMem(),
                              [&](const Block& block) -> void {
                                  ASSERT_EQ(block.size(), 2);
                                  people = *static_cast<ColumnNodeIDs*>(block.columns()[0]);
                                  interests = *static_cast<ColumnNodeIDs*>(block.columns()[1]);
                              });
        ASSERT_TRUE(res);
    }

    for (size_t i = 0; i < people.size(); ++i) {
        interestToPersonMap[interests[i]].emplace_back(people[i]);
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
        QueryStatus res = _db->queryV2(
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
    ColumnNodeIDs people;
    ColumnNodeIDs interests;
    std::unordered_map<NodeID, std::vector<NodeID>> interestToPersonMap;

    {
        constexpr std::string_view scanNodesQuery = "MATCH (n)--(m) RETURN n,m";
        auto res = _db->query(scanNodesQuery, _graphName, &_env->getMem(),
                              [&](const Block& block) -> void {
                                  ASSERT_EQ(block.size(), 2);
                                  people = *static_cast<ColumnNodeIDs*>(block.columns()[0]);
                                  interests = *static_cast<ColumnNodeIDs*>(block.columns()[1]);
                              });
        ASSERT_TRUE(res);
    }

    for (size_t i = 0; i < people.size(); ++i) {
        interestToPersonMap[interests[i]].emplace_back(people[i]);
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
        QueryStatus res = _db->queryV2(
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
    ColumnNodeIDs col1;
    ColumnNodeIDs col2;
    std::unordered_map<NodeID, std::vector<NodeID>> col1ToCol2Map;
    std::unordered_map<NodeID, std::vector<NodeID>> col2ToCol1Map;

    {
        constexpr std::string_view scanNodesQuery = "MATCH (n)--(m) RETURN n,m";
        auto res = _db->query(scanNodesQuery, _graphName, &_env->getMem(),
                              [&](const Block& block) -> void {
                                  ASSERT_EQ(block.size(), 2);
                                  col1 = *static_cast<ColumnNodeIDs*>(block.columns()[0]);
                                  col2 = *static_cast<ColumnNodeIDs*>(block.columns()[1]);
                              });
        ASSERT_TRUE(res);
    }

    for (size_t i = 0; i < col2.size(); ++i) {
        col2ToCol1Map[col2[i]].emplace_back(col1[i]);
        col1ToCol2Map[col1[i]].emplace_back(col2[i]);
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
        QueryStatus res = _db->queryV2(
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
        auto res = _db->queryV2(scanNodesQuery, _graphName, &_env->getMem(),
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
        QueryStatus res = _db->queryV2(
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
    QueryStatus res = _db->queryV2(query, _graphName, &_env->getMem(),
                                   [&]([[maybe_unused]] const Dataframe* df) -> void {});
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

    _db->queryV2("CALL db.labels()", _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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

    _db->queryV2("CALL db.labels() YIELD id, label",
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

    _db->queryV2("CALL db.edgeTypes()", _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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

    _db->queryV2("CALL db.edgeTypes() YIELD id, edgeType",
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

    _db->queryV2("CALL db.propertyTypes()", _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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

    _db->queryV2("CALL db.propertyTypes() YIELD id, propertyType, valueType",
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

    _db->queryV2("CALL db.history()", _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
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

    _db->queryV2("CALL db.history() YIELD commit, nodeCount, edgeCount, partCount",
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

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}
