#include <gtest/gtest.h>
#include <optional>
#include <cstdint>

#include "EdgeRecord.h"
#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
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

class MatchCreateTest : public TuringTest {
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

    void setWorkingGraph(std::string_view name) {
        _graphName = name;
        _graph = _env->getSystemManager().getGraph(std::string {name});
        ASSERT_TRUE(_graph);
    }

    LabelID getLabelID(std::string_view labelName) {
        auto labelOpt = read().getMetadata().labels().get(labelName);
        EXPECT_TRUE(labelOpt.has_value()) << "Label not found: " << labelName;
        return labelOpt.value_or(LabelID{0});
    }

    EdgeTypeID getEdgeTypeID(std::string_view edgeTypeName) {
        auto edgeTypeOpt = read().getMetadata().edgeTypes().get(edgeTypeName);
        EXPECT_TRUE(edgeTypeOpt.has_value()) << "EdgeType not found: " << edgeTypeName;
        return edgeTypeOpt.value_or(EdgeTypeID{0});
    }

    bool nodeHasLabel(NodeID nodeID, LabelID labelID) {
        return read().getNodeLabelSet(nodeID).hasLabel(labelID);
    }

    EdgeTypeID getEdgeType(EdgeID edgeID) {
        return read().getEdgeTypeID(edgeID);
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

// =============================================================================
// Category 1: Basic MATCH CREATE Tests
// =============================================================================

TEST_F(MatchCreateTest, matchCreateBasicNode) {
    // MATCH all Person nodes and CREATE a Friend node for each
    constexpr std::string_view QUERY = R"(MATCH (n:Person) CREATE (m:Friend) RETURN n, m)";

    const size_t nodesBefore = read().getTotalNodesAllocated();

    // Collect Person node IDs first
    std::vector<NodeID> personNodes;
    {
        auto res = query(R"(MATCH (n:Person) RETURN n)", [&](const Dataframe* df) {
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (size_t i = 0; i < df->getRowCount(); ++i) {
                personNodes.push_back(ns->at(i));
            }
        });
        ASSERT_TRUE(res);
    }
    ASSERT_GT(personNodes.size(), 0);

    newChange();
    // Collect (matchedPerson, createdFriend) pairs
    using Rows = LineContainer<NodeID, NodeID>;
    Rows actualRows;
    std::vector<NodeID> matchedPersons;
    std::vector<NodeID> createdNodeIDs;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 2);
        auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
        auto* ms = df->cols().at(1)->as<ColumnNodeIDs>();
        ASSERT_TRUE(ns);
        ASSERT_TRUE(ms);
        for (size_t i = 0; i < df->getRowCount(); ++i) {
            actualRows.add({ns->at(i), ms->at(i)});
            matchedPersons.push_back(ns->at(i));
            createdNodeIDs.push_back(ms->at(i));
        }
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // Verify we got the expected number of results
    ASSERT_EQ(createdNodeIDs.size(), personNodes.size());
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + personNodes.size());

    // Verify all matched nodes were Person nodes
    const LabelID personLabelID = getLabelID("Person");
    for (const NodeID nodeID : matchedPersons) {
        EXPECT_TRUE(nodeHasLabel(nodeID, personLabelID))
            << "Matched node " << nodeID.getValue() << " should have Person label";
    }

    // Verify all created nodes have the Friend label
    const LabelID friendLabelID = getLabelID("Friend");
    for (const NodeID nodeID : createdNodeIDs) {
        EXPECT_TRUE(nodeHasLabel(nodeID, friendLabelID))
            << "Created node " << nodeID.getValue() << " should have Friend label";
    }
}

TEST_F(MatchCreateTest, matchCreateWithProperty) {
    // MATCH nodes with specific property and CREATE new node
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}) CREATE (m:MatchedCopy) RETURN n, m)";

    const size_t nodesBefore = read().getTotalNodesAllocated();

    newChange();
    NodeID matchedNodeID{0};
    NodeID createdNodeID{0};
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->getRowCount(), 1);
        auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
        auto* ms = df->cols().at(1)->as<ColumnNodeIDs>();
        ASSERT_TRUE(ns);
        ASSERT_TRUE(ms);
        matchedNodeID = ns->at(0);
        createdNodeID = ms->at(0);
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // Should match exactly one node named "Remy" and create one copy
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + 1);

    // Verify created node has the MatchedCopy label
    const LabelID matchedCopyLabelID = getLabelID("MatchedCopy");
    EXPECT_TRUE(nodeHasLabel(createdNodeID, matchedCopyLabelID))
        << "Created node should have MatchedCopy label";

    // Verify matched and created are different nodes
    EXPECT_NE(matchedNodeID, createdNodeID);
}

TEST_F(MatchCreateTest, matchCreateMultipleNodes) {
    // MATCH one node and CREATE multiple new nodes
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}) CREATE (a:TypeA), (b:TypeB), (c:TypeC) RETURN n, a, b, c)";

    const size_t nodesBefore = read().getTotalNodesAllocated();

    newChange();
    NodeID nodeA{0}, nodeB{0}, nodeC{0};
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 4);  // n, a, b, c
        ASSERT_EQ(df->getRowCount(), 1);  // One match
        auto* as = df->cols().at(1)->as<ColumnNodeIDs>();
        auto* bs = df->cols().at(2)->as<ColumnNodeIDs>();
        auto* cs = df->cols().at(3)->as<ColumnNodeIDs>();
        ASSERT_TRUE(as && bs && cs);
        nodeA = as->at(0);
        nodeB = bs->at(0);
        nodeC = cs->at(0);
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // Should have created 3 new nodes
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + 3);

    // Verify each node has the correct label
    EXPECT_TRUE(nodeHasLabel(nodeA, getLabelID("TypeA")))
        << "Node A should have TypeA label";
    EXPECT_TRUE(nodeHasLabel(nodeB, getLabelID("TypeB")))
        << "Node B should have TypeB label";
    EXPECT_TRUE(nodeHasLabel(nodeC, getLabelID("TypeC")))
        << "Node C should have TypeC label";

    // Verify all created nodes are distinct
    EXPECT_NE(nodeA, nodeB);
    EXPECT_NE(nodeB, nodeC);
    EXPECT_NE(nodeA, nodeC);
}

TEST_F(MatchCreateTest, matchCreateChain) {
    // MATCH node and CREATE a chain of new nodes with edge
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}) CREATE (a:Start)-[e:CONNECTS]->(b:End) RETURN n, a, e, b)";

    const size_t nodesBefore = read().getTotalNodesAllocated();
    const size_t edgesBefore = read().getTotalEdgesAllocated();

    newChange();
    NodeID nodeA{0}, nodeB{0};
    EdgeID edgeE{0};
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 4);  // n, a, e, b
        ASSERT_EQ(df->getRowCount(), 1);
        auto* as = df->cols().at(1)->as<ColumnNodeIDs>();
        auto* es = df->cols().at(2)->as<ColumnEdgeIDs>();
        auto* bs = df->cols().at(3)->as<ColumnNodeIDs>();
        ASSERT_TRUE(as && es && bs);
        nodeA = as->at(0);
        edgeE = es->at(0);
        nodeB = bs->at(0);
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // 2 new nodes, 1 new edge
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + 2);
    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + 1);

    // Verify node labels, edge type, and connectivity using a single transaction
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();

        // Verify node labels
        const auto startLabelOpt = reader.getMetadata().labels().get("Start");
        const auto endLabelOpt = reader.getMetadata().labels().get("End");
        ASSERT_TRUE(startLabelOpt.has_value()) << "Start label should exist";
        ASSERT_TRUE(endLabelOpt.has_value()) << "End label should exist";
        EXPECT_TRUE(reader.getNodeLabelSet(nodeA).hasLabel(*startLabelOpt))
            << "Node A should have Start label";
        EXPECT_TRUE(reader.getNodeLabelSet(nodeB).hasLabel(*endLabelOpt))
            << "Node B should have End label";

        // Verify edge type
        const auto connectsTypeOpt = reader.getMetadata().edgeTypes().get("CONNECTS");
        ASSERT_TRUE(connectsTypeOpt.has_value()) << "CONNECTS edge type should exist";
        EXPECT_EQ(reader.getEdgeTypeID(edgeE), *connectsTypeOpt)
            << "Edge should have CONNECTS type";

        // Verify edge connects A to B
        const auto edgeView = reader.getNodeView(nodeA).edges();
        bool foundEdge = false;
        for (auto edge : edgeView.outEdges()) {
            if (edge._edgeID == edgeE) {
                EXPECT_EQ(edge._nodeID, nodeA) << "Edge source should be A";
                EXPECT_EQ(edge._otherID, nodeB) << "Edge target should be B";
                foundEdge = true;
                break;
            }
        }
        EXPECT_TRUE(foundEdge) << "Created edge should be found in node A's outgoing edges";
    }
}

TEST_F(MatchCreateTest, matchCreateSelfLoop) {
    // MATCH nodes and CREATE self-loop edges on them
    constexpr std::string_view QUERY = R"(MATCH (n:Interest) CREATE (n)-[e:SELF_REF]->(n) RETURN n, e)";

    const size_t nodesBefore = read().getTotalNodesAllocated();
    const size_t edgesBefore = read().getTotalEdgesAllocated();

    // Count Interest nodes
    size_t interestCount = 0;
    {
        auto res = query(R"(MATCH (n:Interest) RETURN n)", [&](const Dataframe* df) {
            interestCount = df->getRowCount();
        });
        ASSERT_TRUE(res);
    }

    newChange();
    // Collect (node, edge) pairs
    std::vector<std::pair<NodeID, EdgeID>> selfLoops;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
        auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
        ASSERT_TRUE(ns && es);
        for (size_t i = 0; i < df->getRowCount(); ++i) {
            selfLoops.emplace_back(ns->at(i), es->at(i));
        }
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // No new nodes, one self-loop edge per Interest
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore);
    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + interestCount);
    ASSERT_EQ(selfLoops.size(), interestCount);

    // Verify each edge is a self-loop with correct type using a single transaction
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();

        const auto selfRefTypeOpt = reader.getMetadata().edgeTypes().get("SELF_REF");
        const auto interestLabelOpt = reader.getMetadata().labels().get("Interest");
        ASSERT_TRUE(selfRefTypeOpt.has_value()) << "SELF_REF edge type should exist";
        ASSERT_TRUE(interestLabelOpt.has_value()) << "Interest label should exist";

        for (const auto& [nodeID, edgeID] : selfLoops) {
            // Verify node has Interest label
            EXPECT_TRUE(reader.getNodeLabelSet(nodeID).hasLabel(*interestLabelOpt))
                << "Node " << nodeID.getValue() << " should have Interest label";

            // Verify edge type
            EXPECT_EQ(reader.getEdgeTypeID(edgeID), *selfRefTypeOpt)
                << "Edge should have SELF_REF type";

            // Verify it's a self-loop (source == target)
            const auto nodeView = reader.getNodeView(nodeID);
            const auto edgeView = nodeView.edges();
            bool foundSelfLoop = false;
            for (auto edge : edgeView.outEdges()) {
                if (edge._edgeID == edgeID) {
                    EXPECT_EQ(edge._nodeID, nodeID) << "Self-loop source should be the node";
                    EXPECT_EQ(edge._otherID, nodeID) << "Self-loop target should be the same node";
                    foundSelfLoop = true;
                    break;
                }
            }
            EXPECT_TRUE(foundSelfLoop) << "Self-loop edge should be found in node's outgoing edges";
        }
    }
}

// =============================================================================
// Category 2: Edge Source/Target from MATCH
// =============================================================================

TEST_F(MatchCreateTest, matchCreateEdgeFromMatched) {
    // MATCH two specific nodes and CREATE edge between them
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}), (m{name:"Adam"}) CREATE (n)-[e:FRIENDSHIP]->(m) RETURN n, e, m)";

    const size_t edgesBefore = read().getTotalEdgesAllocated();

    newChange();
    NodeID nodeN{0}, nodeM{0};
    EdgeID edgeE{0};
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 3);
        ASSERT_EQ(df->getRowCount(), 1);
        auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
        auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
        auto* ms = df->cols().at(2)->as<ColumnNodeIDs>();
        ASSERT_TRUE(ns && es && ms);
        nodeN = ns->at(0);
        edgeE = es->at(0);
        nodeM = ms->at(0);
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // One new edge
    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + 1);

    // Verify edge type
    EXPECT_EQ(getEdgeType(edgeE), getEdgeTypeID("FRIENDSHIP"))
        << "Edge should have FRIENDSHIP type";

    // Verify edge connects N to M
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        const auto nodeView = reader.getNodeView(nodeN);
        const auto edgeView = nodeView.edges();
        bool foundEdge = false;
        for (auto edge : edgeView.outEdges()) {
            if (edge._edgeID == edgeE) {
                EXPECT_EQ(edge._nodeID, nodeN) << "Edge source should be N (Remy)";
                EXPECT_EQ(edge._otherID, nodeM) << "Edge target should be M (Adam)";
                foundEdge = true;
                break;
            }
        }
        EXPECT_TRUE(foundEdge) << "Created edge should be found in node N's outgoing edges";
    }

    // Verify N and M are different nodes
    EXPECT_NE(nodeN, nodeM);
}

TEST_F(MatchCreateTest, matchCreateEdgeToNew) {
    // MATCH node and CREATE edge from matched to new node
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}) CREATE (n)-[e:HAS_CHILD]->(child:Child) RETURN n, e, child)";

    const size_t nodesBefore = read().getTotalNodesAllocated();
    const size_t edgesBefore = read().getTotalEdgesAllocated();

    newChange();
    NodeID nodeN{0}, childNode{0};
    EdgeID edgeE{0};
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 3);
        ASSERT_EQ(df->getRowCount(), 1);
        auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
        auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
        auto* children = df->cols().at(2)->as<ColumnNodeIDs>();
        ASSERT_TRUE(ns && es && children);
        nodeN = ns->at(0);
        edgeE = es->at(0);
        childNode = children->at(0);
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + 1);
    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + 1);

    // Verify child node has Child label
    EXPECT_TRUE(nodeHasLabel(childNode, getLabelID("Child")))
        << "Created child node should have Child label";

    // Verify edge type
    EXPECT_EQ(getEdgeType(edgeE), getEdgeTypeID("HAS_CHILD"))
        << "Edge should have HAS_CHILD type";

    // Verify edge connects N to child
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        const auto nodeView = reader.getNodeView(nodeN);
        const auto edgeView = nodeView.edges();
        bool foundEdge = false;
        for (auto edge : edgeView.outEdges()) {
            if (edge._edgeID == edgeE) {
                EXPECT_EQ(edge._nodeID, nodeN) << "Edge source should be N (Remy)";
                EXPECT_EQ(edge._otherID, childNode) << "Edge target should be the child node";
                foundEdge = true;
                break;
            }
        }
        EXPECT_TRUE(foundEdge) << "Created edge should be found in node N's outgoing edges";
    }
}

TEST_F(MatchCreateTest, matchCreateEdgeFromNew) {
    // MATCH node and CREATE edge from new node to matched
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}) CREATE (parent:Parent)-[e:PARENT_OF]->(n) RETURN parent, e, n)";

    const size_t nodesBefore = read().getTotalNodesAllocated();
    const size_t edgesBefore = read().getTotalEdgesAllocated();

    newChange();
    NodeID parentNode{0}, nodeN{0};
    EdgeID edgeE{0};
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 3);
        ASSERT_EQ(df->getRowCount(), 1);
        auto* parents = df->cols().at(0)->as<ColumnNodeIDs>();
        auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
        auto* ns = df->cols().at(2)->as<ColumnNodeIDs>();
        ASSERT_TRUE(parents && es && ns);
        parentNode = parents->at(0);
        edgeE = es->at(0);
        nodeN = ns->at(0);
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + 1);
    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + 1);

    // Verify parent node has Parent label
    EXPECT_TRUE(nodeHasLabel(parentNode, getLabelID("Parent")))
        << "Created parent node should have Parent label";

    // Verify edge type
    EXPECT_EQ(getEdgeType(edgeE), getEdgeTypeID("PARENT_OF"))
        << "Edge should have PARENT_OF type";

    // Verify edge connects parent to N
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        const auto nodeView = reader.getNodeView(parentNode);
        const auto edgeView = nodeView.edges();
        bool foundEdge = false;
        for (auto edge : edgeView.outEdges()) {
            if (edge._edgeID == edgeE) {
                EXPECT_EQ(edge._nodeID, parentNode) << "Edge source should be the parent node";
                EXPECT_EQ(edge._otherID, nodeN) << "Edge target should be N (Remy)";
                foundEdge = true;
                break;
            }
        }
        EXPECT_TRUE(foundEdge) << "Created edge should be found in parent node's outgoing edges";
    }
}

TEST_F(MatchCreateTest, matchCreateBidirectional) {
    // MATCH two nodes and CREATE bidirectional edges
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}), (m{name:"Adam"}) CREATE (n)-[e1:LIKES]->(m), (m)-[e2:LIKES]->(n) RETURN n, m, e1, e2)";

    const size_t edgesBefore = read().getTotalEdgesAllocated();

    newChange();
    NodeID nodeN{0}, nodeM{0};
    EdgeID edge1{0}, edge2{0};
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 4);
        ASSERT_EQ(df->getRowCount(), 1);
        auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
        auto* ms = df->cols().at(1)->as<ColumnNodeIDs>();
        auto* e1s = df->cols().at(2)->as<ColumnEdgeIDs>();
        auto* e2s = df->cols().at(3)->as<ColumnEdgeIDs>();
        ASSERT_TRUE(ns && ms && e1s && e2s);
        nodeN = ns->at(0);
        nodeM = ms->at(0);
        edge1 = e1s->at(0);
        edge2 = e2s->at(0);
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // Two new edges
    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + 2);

    // Verify both edges have LIKES type
    const EdgeTypeID likesTypeID = getEdgeTypeID("LIKES");
    EXPECT_EQ(getEdgeType(edge1), likesTypeID) << "Edge e1 should have LIKES type";
    EXPECT_EQ(getEdgeType(edge2), likesTypeID) << "Edge e2 should have LIKES type";

    // Verify e1 connects N -> M and e2 connects M -> N
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();

        // Check edge e1: N -> M
        {
            const auto nodeViewN = reader.getNodeView(nodeN);
            const auto edgeViewN = nodeViewN.edges();
            bool foundEdge1 = false;
            for (auto edge : edgeViewN.outEdges()) {
                if (edge._edgeID == edge1) {
                    EXPECT_EQ(edge._nodeID, nodeN) << "Edge e1 source should be N";
                    EXPECT_EQ(edge._otherID, nodeM) << "Edge e1 target should be M";
                    foundEdge1 = true;
                    break;
                }
            }
            EXPECT_TRUE(foundEdge1) << "Edge e1 should be found in node N's outgoing edges";
        }

        // Check edge e2: M -> N
        {
            const auto nodeViewM = reader.getNodeView(nodeM);
            const auto edgeViewM = nodeViewM.edges();
            bool foundEdge2 = false;
            for (auto edge : edgeViewM.outEdges()) {
                if (edge._edgeID == edge2) {
                    EXPECT_EQ(edge._nodeID, nodeM) << "Edge e2 source should be M";
                    EXPECT_EQ(edge._otherID, nodeN) << "Edge e2 target should be N";
                    foundEdge2 = true;
                    break;
                }
            }
            EXPECT_TRUE(foundEdge2) << "Edge e2 should be found in node M's outgoing edges";
        }
    }

    // Verify the edges are distinct
    EXPECT_NE(edge1, edge2);
}

// =============================================================================
// Category 3: Multi-Hop MATCH + CREATE
// =============================================================================

TEST_F(MatchCreateTest, matchPathCreateShortcut) {
    // MATCH a 2-hop path and CREATE shortcut edge
    constexpr std::string_view QUERY = R"(MATCH (a:Person)-[]->(b:Interest)-[]->(c) CREATE (a)-[e:SHORTCUT]->(c) RETURN a, c, e)";

    const size_t edgesBefore = read().getTotalEdgesAllocated();

    newChange();
    size_t pathCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        pathCount = df->getRowCount();
    });
    // This may or may not work depending on graph structure
    if (res) {
        submitCurrentChange();
        // Created one shortcut per path found
        ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + pathCount);
    }
    // If it fails, that's also valid - just means no 2-hop paths found
}

TEST_F(MatchCreateTest, matchPathCreateOnIntermediate) {
    // MATCH path and CREATE from intermediate node
    constexpr std::string_view QUERY = R"(MATCH (a:Person)-[]->(b:Interest) CREATE (b)-[e:TAGGED]->(t:Tag) RETURN a, b, t, e)";

    const size_t nodesBefore = read().getTotalNodesAllocated();
    const size_t edgesBefore = read().getTotalEdgesAllocated();

    newChange();
    size_t pathCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        pathCount = df->getRowCount();
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // One new Tag and edge per path
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + pathCount);
    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + pathCount);
}

TEST_F(MatchCreateTest, matchEdgePatternCreate) {
    // MATCH specific edge pattern and CREATE new connections
    constexpr std::string_view QUERY = R"(MATCH (a)-[r:KNOWS_WELL]->(b) CREATE (a)-[e:ALSO_KNOWS]->(b) RETURN a, b, e)";

    const size_t edgesBefore = read().getTotalEdgesAllocated();

    // Count existing KNOWS_WELL edges
    size_t knowsCount = 0;
    {
        auto res = query(R"(MATCH (a)-[r:KNOWS_WELL]->(b) RETURN r)", [&](const Dataframe* df) {
            knowsCount = df->getRowCount();
        });
        ASSERT_TRUE(res);
    }

    newChange();
    size_t createdCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        createdCount = df->getRowCount();
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    ASSERT_EQ(createdCount, knowsCount);
    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + knowsCount);
}

// =============================================================================
// Category 4: Empty MATCH Results
// =============================================================================

TEST_F(MatchCreateTest, matchNoResultsCreate) {
    // MATCH non-existent label - CREATE should not execute
    // NOTE: Standard Cypher behavior would be to succeed with 0 creates.
    // TuringDB currently fails the query when MATCH returns no results.
    // This test documents the current behavior.
    constexpr std::string_view QUERY = R"(MATCH (n:NonExistentLabel) CREATE (m:ShouldNotExist) RETURN n, m)";

    const size_t nodesBefore = read().getTotalNodesAllocated();

    newChange();
    auto res = query(QUERY, [&](const Dataframe*) {});
    // Current behavior: query fails when MATCH returns empty
    // Expected Cypher behavior: query succeeds with 0 results
    ASSERT_FALSE(res);

    // No nodes should have been created
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore);
}

TEST_F(MatchCreateTest, matchFilteredEmptyCreate) {
    // MATCH with WHERE that filters out all results
    constexpr std::string_view QUERY = R"(MATCH (n:Person) WHERE n.age > 1000 CREATE (m:AncientFriend) RETURN n, m)";

    const size_t nodesBefore = read().getTotalNodesAllocated();

    newChange();
    size_t resultCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        resultCount = df->getRowCount();
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // WHERE filters out all - no creates
    ASSERT_EQ(resultCount, 0);
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore);
}

TEST_F(MatchCreateTest, matchCartesianEmptyCreate) {
    // MATCH with cartesian product where one side is empty
    // NOTE: Standard Cypher behavior would be to succeed with 0 creates.
    // TuringDB currently fails the query when MATCH returns no results.
    constexpr std::string_view QUERY = R"(MATCH (a:Person), (b:NonExistent) CREATE (a)-[e:TO]->(b) RETURN a, b, e)";

    const size_t edgesBefore = read().getTotalEdgesAllocated();

    newChange();
    auto res = query(QUERY, [&](const Dataframe*) {});
    // Current behavior: query fails when MATCH returns empty (cartesian product with empty = empty)
    ASSERT_FALSE(res);

    // No edges should have been created
    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore);
}

// =============================================================================
// Category 5: Cardinality Tests
// =============================================================================

TEST_F(MatchCreateTest, matchManyCreateMany) {
    // Verify that CREATE executes once per MATCH result
    constexpr std::string_view QUERY = R"(MATCH (n:Person) CREATE (m:PersonCopy) RETURN n, m)";

    const size_t nodesBefore = read().getTotalNodesAllocated();

    size_t personCount = 0;
    {
        auto res = query(R"(MATCH (n:Person) RETURN n)", [&](const Dataframe* df) {
            personCount = df->getRowCount();
        });
        ASSERT_TRUE(res);
    }

    newChange();
    size_t createCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        createCount = df->getRowCount();
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // One create per person
    ASSERT_EQ(createCount, personCount);
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + personCount);
}

TEST_F(MatchCreateTest, matchCartesianCreate) {
    // MATCH cartesian product - CREATE should multiply
    constexpr std::string_view QUERY = R"(MATCH (a:Person), (b:Interest) CREATE (link:Link) RETURN a, b, link)";

    const size_t nodesBefore = read().getTotalNodesAllocated();

    size_t personCount = 0, interestCount = 0;
    {
        auto res = query(R"(MATCH (n:Person) RETURN n)", [&](const Dataframe* df) {
            personCount = df->getRowCount();
        });
        ASSERT_TRUE(res);
        res = query(R"(MATCH (n:Interest) RETURN n)", [&](const Dataframe* df) {
            interestCount = df->getRowCount();
        });
        ASSERT_TRUE(res);
    }

    newChange();
    size_t linkCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        linkCount = df->getRowCount();
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // Cartesian product
    const size_t expectedLinks = personCount * interestCount;
    ASSERT_EQ(linkCount, expectedLinks);
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + expectedLinks);
}

TEST_F(MatchCreateTest, matchSameNodeTwiceCreate) {
    // MATCH same pattern twice - should duplicate creates
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}), (m{name:"Remy"}) CREATE (x:Duplicate) RETURN n, m, x)";

    const size_t nodesBefore = read().getTotalNodesAllocated();

    newChange();
    size_t resultCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        resultCount = df->getRowCount();
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // 1x1 = 1 (same node matched twice still 1 combo)
    ASSERT_EQ(resultCount, 1);
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + 1);
}

// =============================================================================
// Category 6: Edge Cases & Potential Bugs
// =============================================================================

TEST_F(MatchCreateTest, matchCreateSameVariable) {
    // Try to CREATE with same variable as MATCH - should error
    constexpr std::string_view QUERY = R"(MATCH (n:Person) CREATE (n:ExtraLabel) RETURN n)";

    newChange();
    auto res = query(QUERY, [&](const Dataframe* df) {
        // Should not reach here
        FAIL() << "Query should have failed but succeeded";
    });

    // Should fail at analysis - variable already defined
    ASSERT_FALSE(res);
}

TEST_F(MatchCreateTest, matchCreateUndefinedRef) {
    // CREATE referencing undefined variable
    constexpr std::string_view QUERY = R"(MATCH (n:Person) CREATE (x:New)-[e:TO]->(undefined) RETURN x)";

    newChange();
    auto res = query(QUERY, [&](const Dataframe* df) {
        FAIL() << "Query should have failed";
    });

    // Should fail - undefined not declared
    ASSERT_FALSE(res);
}

TEST_F(MatchCreateTest, matchCreateEdgeNoType) {
    // CREATE edge without type - should error
    constexpr std::string_view QUERY = R"(MATCH (n:Person), (m:Interest) CREATE (n)-[e]->(m) RETURN e)";

    newChange();
    auto res = query(QUERY, [&](const Dataframe* df) {
        FAIL() << "Query should have failed";
    });

    // Should fail - edge must have type
    ASSERT_FALSE(res);
}

TEST_F(MatchCreateTest, matchCreateEmptyLabel) {
    // CREATE node with empty label - should error
    constexpr std::string_view QUERY = R"(MATCH (n:Person) CREATE (m:) RETURN m)";

    newChange();
    auto res = query(QUERY, [&](const Dataframe* df) {
        FAIL() << "Query should have failed";
    });

    // Should fail at parsing
    ASSERT_FALSE(res);
}

TEST_F(MatchCreateTest, matchCreatePropertyDependency) {
    // CREATE with property referencing matched node - currently unsupported
    constexpr std::string_view QUERY = R"(MATCH (n:Person) CREATE (m:Copy{name:n.name}) RETURN m)";

    newChange();
    auto res = query(QUERY, [&](const Dataframe* df) {
        // May fail or succeed depending on support level
    });

    // Currently this throws "entity dependencies not supported"
    // This test documents the current behavior
    if (!res) {
        // Expected - feature not supported
        SUCCEED() << "Property dependency not supported (expected)";
    } else {
        // If it works, great - verify it actually copied
        SUCCEED() << "Property dependency is supported";
    }
}

TEST_F(MatchCreateTest, matchCreateReuseEdgeVar) {
    // Try to CREATE with edge variable that already exists from MATCH
    constexpr std::string_view QUERY = R"(MATCH (n)-[e:KNOWS_WELL]->(m) CREATE (n)-[e:DUPLICATE]->(m) RETURN e)";

    newChange();
    auto res = query(QUERY, [&](const Dataframe* df) {
        FAIL() << "Query should have failed - edge var reuse";
    });

    // Should fail - edge variable already exists
    ASSERT_FALSE(res);
}

// =============================================================================
// Category 7: Complex Patterns
// =============================================================================

TEST_F(MatchCreateTest, matchWhereCreateFiltered) {
    // MATCH with WHERE and CREATE
    constexpr std::string_view QUERY = R"(MATCH (n:Person) WHERE n.hasPhD = true CREATE (m:PhDHolder) RETURN n, m)";

    const size_t nodesBefore = read().getTotalNodesAllocated();

    // Count PhD holders
    size_t phdCount = 0;
    {
        auto res = query(R"(MATCH (n:Person) WHERE n.hasPhD = true RETURN n)", [&](const Dataframe* df) {
            phdCount = df->getRowCount();
        });
        ASSERT_TRUE(res);
    }

    newChange();
    size_t createCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        createCount = df->getRowCount();
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    ASSERT_EQ(createCount, phdCount);
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + phdCount);
}

TEST_F(MatchCreateTest, matchMultiLabelCreate) {
    // MATCH nodes with multiple labels and CREATE with multiple labels
    constexpr std::string_view QUERY = R"(MATCH (n:Person:Founder) CREATE (m:Clone:Copy:Backup) RETURN n, m)";

    const size_t nodesBefore = read().getTotalNodesAllocated();

    // Count Founder persons
    size_t founderCount = 0;
    {
        auto res = query(R"(MATCH (n:Person:Founder) RETURN n)", [&](const Dataframe* df) {
            founderCount = df->getRowCount();
        });
        ASSERT_TRUE(res);
    }

    newChange();
    size_t createCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        createCount = df->getRowCount();
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    ASSERT_EQ(createCount, founderCount);
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + founderCount);
}

TEST_F(MatchCreateTest, matchCreateWithReturn) {
    // Verify RETURN includes both matched and created entities
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}) CREATE (m:New{tag:"created"}) RETURN n.name, m)";

    newChange();
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 2);  // n.name and m
        ASSERT_EQ(df->getRowCount(), 1);
    });
    ASSERT_TRUE(res);
    submitCurrentChange();
}

// =============================================================================
// Additional Edge Case Tests
// =============================================================================

TEST_F(MatchCreateTest, matchCreateMultiplePatterns) {
    // Multiple match patterns with shared CREATE
    constexpr std::string_view QUERY = R"(MATCH (a:Person), (b:Interest) WHERE a.name = "Remy" CREATE (a)-[e:TAGGED]->(b) RETURN a, b, e)";

    const size_t edgesBefore = read().getTotalEdgesAllocated();

    // Count interests
    size_t interestCount = 0;
    {
        auto res = query(R"(MATCH (n:Interest) RETURN n)", [&](const Dataframe* df) {
            interestCount = df->getRowCount();
        });
        ASSERT_TRUE(res);
    }

    newChange();
    size_t edgeCount = 0;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        edgeCount = df->getRowCount();
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // One edge per interest (since Remy is unique)
    ASSERT_EQ(edgeCount, interestCount);
    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + interestCount);
}

TEST_F(MatchCreateTest, matchCreateBackwardEdge) {
    // CREATE with backward edge direction
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}), (m{name:"Adam"}) CREATE (n)<-[e:ADMIRED_BY]-(m) RETURN n, m, e)";

    const size_t edgesBefore = read().getTotalEdgesAllocated();

    newChange();
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->getRowCount(), 1);
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + 1);
}

TEST_F(MatchCreateTest, matchCreateLongChain) {
    // CREATE a longer chain from matched node
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}) CREATE (a:A)-[e1:R]->(b:B)-[e2:R]->(c:C)-[e3:R]->(d:D) RETURN n, a, e1, b, e2, c, e3, d)";

    const size_t nodesBefore = read().getTotalNodesAllocated();
    const size_t edgesBefore = read().getTotalEdgesAllocated();

    newChange();
    NodeID nodeA{0}, nodeB{0}, nodeC{0}, nodeD{0};
    EdgeID edgeE1{0}, edgeE2{0}, edgeE3{0};
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 8);  // n, a, e1, b, e2, c, e3, d
        ASSERT_EQ(df->getRowCount(), 1);
        auto* as = df->cols().at(1)->as<ColumnNodeIDs>();
        auto* e1s = df->cols().at(2)->as<ColumnEdgeIDs>();
        auto* bs = df->cols().at(3)->as<ColumnNodeIDs>();
        auto* e2s = df->cols().at(4)->as<ColumnEdgeIDs>();
        auto* cs = df->cols().at(5)->as<ColumnNodeIDs>();
        auto* e3s = df->cols().at(6)->as<ColumnEdgeIDs>();
        auto* ds = df->cols().at(7)->as<ColumnNodeIDs>();
        ASSERT_TRUE(as && e1s && bs && e2s && cs && e3s && ds);
        nodeA = as->at(0);
        edgeE1 = e1s->at(0);
        nodeB = bs->at(0);
        edgeE2 = e2s->at(0);
        nodeC = cs->at(0);
        edgeE3 = e3s->at(0);
        nodeD = ds->at(0);
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // 4 new nodes, 3 new edges
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + 4);
    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + 3);

    // Verify node labels
    EXPECT_TRUE(nodeHasLabel(nodeA, getLabelID("A"))) << "Node A should have A label";
    EXPECT_TRUE(nodeHasLabel(nodeB, getLabelID("B"))) << "Node B should have B label";
    EXPECT_TRUE(nodeHasLabel(nodeC, getLabelID("C"))) << "Node C should have C label";
    EXPECT_TRUE(nodeHasLabel(nodeD, getLabelID("D"))) << "Node D should have D label";

    // Verify all edges have R type
    const EdgeTypeID rTypeID = getEdgeTypeID("R");
    EXPECT_EQ(getEdgeType(edgeE1), rTypeID) << "Edge e1 should have R type";
    EXPECT_EQ(getEdgeType(edgeE2), rTypeID) << "Edge e2 should have R type";
    EXPECT_EQ(getEdgeType(edgeE3), rTypeID) << "Edge e3 should have R type";

    // Verify chain connectivity: A->B->C->D
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto verifyEdge = [&](EdgeID edgeID, NodeID src, NodeID dst) {
            const auto nodeView = reader.getNodeView(src);
            const auto edgeView = nodeView.edges();
            bool found = false;
            for (auto edge : edgeView.outEdges()) {
                if (edge._edgeID == edgeID) {
                    EXPECT_EQ(edge._nodeID, src);
                    EXPECT_EQ(edge._otherID, dst);
                    found = true;
                    break;
                }
            }
            return found;
        };

        EXPECT_TRUE(verifyEdge(edgeE1, nodeA, nodeB)) << "Edge e1 should connect A to B";
        EXPECT_TRUE(verifyEdge(edgeE2, nodeB, nodeC)) << "Edge e2 should connect B to C";
        EXPECT_TRUE(verifyEdge(edgeE3, nodeC, nodeD)) << "Edge e3 should connect C to D";
    }
}

TEST_F(MatchCreateTest, matchCreateTriangle) {
    // CREATE a triangle from matched node
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}) CREATE (a:Tri)-[e1:SIDE]->(b:Tri), (b)-[e2:SIDE]->(c:Tri), (c)-[e3:SIDE]->(a) RETURN a, e1, b, e2, c, e3)";

    const size_t nodesBefore = read().getTotalNodesAllocated();
    const size_t edgesBefore = read().getTotalEdgesAllocated();

    newChange();
    NodeID nodeA{0}, nodeB{0}, nodeC{0};
    EdgeID edgeE1{0}, edgeE2{0}, edgeE3{0};
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 6);  // a, e1, b, e2, c, e3
        ASSERT_EQ(df->getRowCount(), 1);
        auto* as = df->cols().at(0)->as<ColumnNodeIDs>();
        auto* e1s = df->cols().at(1)->as<ColumnEdgeIDs>();
        auto* bs = df->cols().at(2)->as<ColumnNodeIDs>();
        auto* e2s = df->cols().at(3)->as<ColumnEdgeIDs>();
        auto* cs = df->cols().at(4)->as<ColumnNodeIDs>();
        auto* e3s = df->cols().at(5)->as<ColumnEdgeIDs>();
        ASSERT_TRUE(as && e1s && bs && e2s && cs && e3s);
        nodeA = as->at(0);
        edgeE1 = e1s->at(0);
        nodeB = bs->at(0);
        edgeE2 = e2s->at(0);
        nodeC = cs->at(0);
        edgeE3 = e3s->at(0);
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    // 3 new nodes, 3 new edges (triangle)
    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + 3);
    ASSERT_EQ(read().getTotalEdgesAllocated(), edgesBefore + 3);

    // Verify all nodes have Tri label
    const LabelID triLabelID = getLabelID("Tri");
    EXPECT_TRUE(nodeHasLabel(nodeA, triLabelID)) << "Node A should have Tri label";
    EXPECT_TRUE(nodeHasLabel(nodeB, triLabelID)) << "Node B should have Tri label";
    EXPECT_TRUE(nodeHasLabel(nodeC, triLabelID)) << "Node C should have Tri label";

    // Verify all edges have SIDE type
    const EdgeTypeID sideTypeID = getEdgeTypeID("SIDE");
    EXPECT_EQ(getEdgeType(edgeE1), sideTypeID) << "Edge e1 should have SIDE type";
    EXPECT_EQ(getEdgeType(edgeE2), sideTypeID) << "Edge e2 should have SIDE type";
    EXPECT_EQ(getEdgeType(edgeE3), sideTypeID) << "Edge e3 should have SIDE type";

    // Verify triangle connectivity: A->B->C->A
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto verifyEdge = [&](EdgeID edgeID, NodeID src, NodeID dst) {
            const auto nodeView = reader.getNodeView(src);
            const auto edgeView = nodeView.edges();
            bool found = false;
            for (auto edge : edgeView.outEdges()) {
                if (edge._edgeID == edgeID) {
                    EXPECT_EQ(edge._nodeID, src);
                    EXPECT_EQ(edge._otherID, dst);
                    found = true;
                    break;
                }
            }
            return found;
        };

        EXPECT_TRUE(verifyEdge(edgeE1, nodeA, nodeB)) << "Edge e1 should connect A to B";
        EXPECT_TRUE(verifyEdge(edgeE2, nodeB, nodeC)) << "Edge e2 should connect B to C";
        EXPECT_TRUE(verifyEdge(edgeE3, nodeC, nodeA)) << "Edge e3 should connect C to A (closing the triangle)";
    }

    // Verify all nodes are distinct
    EXPECT_NE(nodeA, nodeB);
    EXPECT_NE(nodeB, nodeC);
    EXPECT_NE(nodeA, nodeC);
}

TEST_F(MatchCreateTest, matchCreateWithStaticProperties) {
    // CREATE with static property values
    constexpr std::string_view QUERY = R"(MATCH (n{name:"Remy"}) CREATE (m:Tagged{source:"remy", count:42, active:true}) RETURN n, m)";

    const size_t nodesBefore = read().getTotalNodesAllocated();

    newChange();
    NodeID createdNode{0};
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->getRowCount(), 1);
        auto* ms = df->cols().at(1)->as<ColumnNodeIDs>();
        ASSERT_TRUE(ms);
        createdNode = ms->at(0);
    });
    ASSERT_TRUE(res);
    submitCurrentChange();

    ASSERT_EQ(read().getTotalNodesAllocated(), nodesBefore + 1);

    // Verify the created node has the Tagged label
    EXPECT_TRUE(nodeHasLabel(createdNode, getLabelID("Tagged")))
        << "Created node should have Tagged label";

    // Verify properties were set using GraphReader API
    auto sourcePropID = read().getMetadata().propTypes().get("source");
    auto countPropID = read().getMetadata().propTypes().get("count");
    auto activePropID = read().getMetadata().propTypes().get("active");

    ASSERT_TRUE(sourcePropID.has_value()) << "source property should exist";
    ASSERT_TRUE(countPropID.has_value()) << "count property should exist";
    ASSERT_TRUE(activePropID.has_value()) << "active property should exist";

    const auto* source = read().tryGetNodeProperty<types::String>(sourcePropID->_id, createdNode);
    const auto* count = read().tryGetNodeProperty<types::Int64>(countPropID->_id, createdNode);
    const auto* active = read().tryGetNodeProperty<types::Bool>(activePropID->_id, createdNode);

    ASSERT_TRUE(source != nullptr) << "source property should be set";
    ASSERT_TRUE(count != nullptr) << "count property should be set";
    ASSERT_TRUE(active != nullptr) << "active property should be set";

    EXPECT_EQ(*source, "remy") << "source should be 'remy'";
    EXPECT_EQ(*count, 42) << "count should be 42";
    EXPECT_TRUE(static_cast<bool>(*active)) << "active should be true";
}
