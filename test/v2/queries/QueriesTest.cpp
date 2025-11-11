#include <gtest/gtest.h>

#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
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
    const std::string query = "MATCH (n)-->(m) RETURN n";

    std::vector<NodeID> returnedTargets;
    std::vector<NodeID> returnedSources;
    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 4);
        ASSERT_EQ(df->size(), 4);

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
        returnedTargets.insert(returnedTargets.end(), targetIDs->begin(), targetIDs->end());
        returnedSources.insert(returnedSources.end(), sourceIDs->begin(), sourceIDs->end());
    });

    // Get all expected node IDs
    std::vector<NodeID> expectedSources;
    std::vector<NodeID> expectedTargets;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            const auto edgeView = reader.getNodeView(node).edges();
            for (auto edge : edgeView.outEdges()) {
                expectedSources.push_back(node);
                expectedTargets.push_back(edge._otherID);
            }
        }
    }

    ASSERT_FALSE(returnedSources.empty());
    ASSERT_FALSE(returnedTargets.empty());
    ASSERT_EQ(returnedSources, expectedSources);
    ASSERT_EQ(returnedTargets, expectedTargets);
}

TEST_F(QueriesTest, scanExpand2) {
    const std::string query = "MATCH (n)-->(m)-->(t) RETURN n";

    std::vector<NodeID> returnedTargets1;
    std::vector<NodeID> returnedTargets2;
    std::vector<NodeID> returnedSources;
    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 7);
        ASSERT_EQ(df->size(), 7);

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
    const std::string query = "MATCH (n)<--(m) RETURN n";

    LineContainer<NodeID,NodeID> returned;
    LineContainer<NodeID,NodeID> expected;
    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 4);
        ASSERT_EQ(df->size(), 4);

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
    const std::string query = "MATCH (n)<-[e1]-(m)<-[e2]-(t) RETURN n";

    LineContainer<NodeID, EdgeID, NodeID, EdgeID, NodeID> returnedLines;
    LineContainer<NodeID, EdgeID, NodeID, EdgeID, NodeID> expectedLines;
    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 7);
        ASSERT_EQ(df->size(), 7);

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
    const std::string query = "MATCH (n)-[e]-(m) RETURN n";

    LineContainer<NodeID, EdgeID, NodeID> returnedLines;
    LineContainer<NodeID, EdgeID, NodeID> expectedLines;
    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 4);
        ASSERT_EQ(df->size(), 4);

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
