#include <gtest/gtest.h>

#include <set>
#include <string>

#include "TuringDB.h"
#include "Graph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"
#include "EdgeRecord.h"

#include "CypherAST.h"
#include "CypherParser.h"
#include "CypherAnalyzer.h"
#include "PlanGraph.h"
#include "PlanGraphGenerator.h"
#include "PlanOptimizer.h"
#include "PipelineGenerator.h"
#include "PipelineV2.h"
#include "PipelineExecutor.h"
#include "ExecutionContext.h"
#include "procedures/ProcedureManager.h"
#include "nodes/PlanGraphNode.h"

#include "LineContainer.h"
#include "TuringTestEnv.h"
#include "TuringTest.h"
#include "TuringConfig.h"

using namespace turing::test;

// Custom interpreter that exposes the optimized plan graph.
class TestQueryInterpreter {
public:
    TestQueryInterpreter(db::SystemManager* sysMan,
                         std::string_view graphName,
                         const db::QueryConfig* queryConfig)
        : _sysMan(sysMan),
        _graphName(graphName),
        _queryConfig(queryConfig)
    {
    }

    void execute(std::string_view cypher, auto callback) {
        executeImpl(cypher, db::ChangeID::head(), callback);
    }

    void execute(std::string_view cypher, db::ChangeID change, auto callback) {
        executeImpl(cypher, change, callback);
    }

private:
    void executeImpl(std::string_view cypher, db::ChangeID change, auto callback) {
        _procedures = db::ProcedureManager::create();
        _procedures->init();

        auto txRes = _sysMan->openTransaction(_graphName,
                                              db::CommitHash::head(),
                                              change);
        _tx.emplace(std::move(txRes.value()));
        const db::GraphView view = _tx->viewGraph();

        _ast.emplace(_procedures.get(), cypher);
        db::CypherParser parser(&*_ast);
        parser.parse(cypher);

        db::CypherAnalyzer analyzer(&*_ast, view);
        analyzer.analyze();

        _planGen.emplace(*_ast, view, &_queryConfig->getPlanGenConfig());
        _planGen->generate(_ast->queries().front());

        db::PlanGraph& plan = _planGen->getPlanGraph();

        db::LocalMemory mem;
        db::PlanOptimizer opt(&plan, view, &mem, &*_ast);
        opt.optimize();

        db::QueryCallbacks callbacks;
        callbacks.setOnOutputData([&callback](const db::Dataframe* df) {
            callback(df);
        });

        db::PipelineV2 pipeline;
        db::PipelineGenerator pipelineGen(&plan, view, &pipeline, &mem,
                                          _sysMan, _procedures.get(), &callbacks);
        pipelineGen.generate();

        db::ExecutionContext execCtxt(_sysMan, view);
        execCtxt.setTransaction(&*_tx);
        execCtxt.setGraphName(_graphName);
        execCtxt.setProcedures(_procedures.get());

        db::PipelineExecutor executor(&pipeline, &execCtxt);
        executor.execute();
    }

public:
    bool planHasOpcode(db::PlanGraphOpcode opcode) {
        if (!_planGen) {
            return false;
        }
        for (const auto& node : _planGen->getPlanGraph().nodes()) {
            if (node->getOpcode() == opcode) {
                return true;
            }
        }
        return false;
    }

private:
    db::SystemManager* _sysMan {nullptr};
    std::string_view _graphName;
    const db::QueryConfig* _queryConfig {nullptr};

    std::unique_ptr<db::ProcedureManager> _procedures;
    std::optional<db::Transaction> _tx;
    std::optional<db::CypherAST> _ast;
    std::optional<db::PlanGraphGenerator> _planGen;
};

// ---------------------------------------------------------------------------
// Build a connected graph that resembles a small reactome:
//
//   100 Pathway nodes, 200 Reaction nodes, 300 Entity nodes
//
//   Edges:
//     Pathway spine:     pathway_i --hasEvent--> pathway_i+1
//     Pathway->Reaction: each pathway fans out to 2 reactions
//     Reaction->Entity:  each reaction fans out to 2 entities
//     Entity ring:       entity_i --surroundedBy--> entity_(i+1) mod 300
//
//   Total: 600 nodes, ~1100 edges
// ---------------------------------------------------------------------------

class ConstScanOptTest : public TuringTest {
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _env->getConfig().setSyncedOnDisk(false);
        _env->getSystemManager().createGraph("default");
        _graph = _env->getSystemManager().createGraph(_graphName);
        _db = &_env->getDB();

        buildGraph();
    }

protected:
    const std::string _graphName = "biograph";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    ChangeID _currentChange {ChangeID::head()};

    GraphReader read() { return _graph->openTransaction().readGraph(); }

    void newChange() {
        auto res = _env->getSystemManager().newChange(_graphName);
        ASSERT_TRUE(res);
        _currentChange = res.value()->id();
    }

    void submitCurrentChange() {
        auto res = _db->query("change submit", _graphName, &_env->getMem(),
                              &_db->getDefaultQueryConfig(),
                              CommitHash::head(), _currentChange);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    auto query(std::string_view query, auto callback) {
        auto res = _db->query(query, _graphName, &_env->getMem(),
                              &_db->getDefaultQueryConfig(),
                              callback, CommitHash::head(), _currentChange);
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

    void exec(std::string_view q) {
        auto res = _db->query(q, _graphName, &_env->getMem(),
                              &_db->getDefaultQueryConfig(),
                              [](const Dataframe*) {},
                              CommitHash::head(), _currentChange);
        ASSERT_TRUE(res);
    }

    void buildGraph() {
        newChange();

        for (int i = 0; i < 100; ++i) {
            exec(fmt::format(R"(CREATE (:Pathway{{idx:{}}}))", i));
        }
        for (int i = 0; i < 200; ++i) {
            exec(fmt::format(R"(CREATE (:Reaction{{idx:{}}}))", i));
        }
        for (int i = 0; i < 300; ++i) {
            exec(fmt::format(R"(CREATE (:Entity{{idx:{}}}))", i));
        }

        submitCurrentChange();
        newChange();

        // Pathway spine: pathway_i -> pathway_i+1
        for (int i = 0; i < 99; ++i) {
            exec(fmt::format(
                R"(MATCH (a:Pathway{{idx:{}}}), (b:Pathway{{idx:{}}}) CREATE (a)-[:hasEvent]->(b))",
                i, i + 1));
        }

        submitCurrentChange();
        newChange();

        // Pathway -> 2 Reactions each
        for (int p = 0; p < 100; ++p) {
            const int r0 = (p * 2) % 200;
            const int r1 = (p * 2 + 1) % 200;
            exec(fmt::format(
                R"(MATCH (a:Pathway{{idx:{}}}), (b:Reaction{{idx:{}}}) CREATE (a)-[:hasEvent]->(b))",
                p, r0));
            exec(fmt::format(
                R"(MATCH (a:Pathway{{idx:{}}}), (b:Reaction{{idx:{}}}) CREATE (a)-[:hasEvent]->(b))",
                p, r1));
        }

        submitCurrentChange();
        newChange();

        // Reaction -> 2 Entities each
        for (int r = 0; r < 200; ++r) {
            const int e0 = (r * 3) % 300;
            const int e1 = (r * 3 + 1) % 300;
            exec(fmt::format(
                R"(MATCH (a:Reaction{{idx:{}}}), (b:Entity{{idx:{}}}) CREATE (a)-[:output]->(b))",
                r, e0));
            exec(fmt::format(
                R"(MATCH (a:Reaction{{idx:{}}}), (b:Entity{{idx:{}}}) CREATE (a)-[:output]->(b))",
                r, e1));
        }

        submitCurrentChange();
        newChange();

        // Entity ring: entity_i -> entity_(i+1) mod 300
        for (int i = 0; i < 300; ++i) {
            exec(fmt::format(
                R"(MATCH (a:Entity{{idx:{}}}), (b:Entity{{idx:{}}}) CREATE (a)-[:surroundedBy]->(b))",
                i, (i + 1) % 300));
        }

        submitCurrentChange();
    }

    // Expand one hop: for each source, append all out-edge targets.
    // Preserves per-path multiplicity.
    void expandOneHop(std::vector<NodeID>& out, const std::vector<NodeID>& sources) {
        const GraphReader reader = read();
        for (const NodeID src : sources) {
            const NodeView view = reader.getNodeView(src);
            for (const EdgeRecord& e : view.edges().outEdges()) {
                out.push_back(e._otherID);
            }
        }
    }

    static std::string makeOrChain(std::string_view var,
                                   const std::vector<NodeID>& seeds) {
        std::string chain;
        for (size_t i = 0; i < seeds.size(); ++i) {
            if (i > 0) {
                chain += " OR ";
            }
            chain += fmt::format("{} = {}", var, seeds[i].getValue());
        }
        return chain;
    }
};

TEST_F(ConstScanOptTest, multiHopSeeds) {
    // Grab the first 10 Pathway NodeIDs by scanning the graph
    const LabelID pathwayLabel = read().getView().metadata().labels().get("Pathway").value();
    LabelSet pathwayLabelSet;
    pathwayLabelSet.set(pathwayLabel);
    const LabelSetHandle pathwayHandle(pathwayLabelSet);

    std::vector<NodeID> seeds;
    for (const NodeID nid : read().scanNodesByLabel(pathwayHandle)) {
        seeds.push_back(nid);
        if (seeds.size() == 10) {
            break;
        }
    }
    ASSERT_EQ(seeds.size(), 10);

    const std::string orChain = makeOrChain("n", seeds);

    // Queries to test at each hop depth
    const std::string q0 = fmt::format("MATCH (n) WHERE {} RETURN n", orChain);
    const std::string q1 = fmt::format("MATCH (n)-->(m) WHERE {} RETURN m", orChain);
    const std::string q2 = fmt::format("MATCH (n)-->(m)-->(p) WHERE {} RETURN p", orChain);
    const std::string q3 = fmt::format("MATCH (n)-->(m)-->(p)-->(q) WHERE {} RETURN q", orChain);

    // === Zero hops ===
    {
        using Rows = LineContainer<NodeID>;
        Rows expected;
        for (const NodeID nid : seeds) {
            expected.add({nid});
        }

        Rows actual;
        TestQueryInterpreter interp(&_env->getSystemManager(),
                                    _graphName,
                                    &_db->getDefaultQueryConfig());
        interp.execute(q0, [&](const Dataframe* df) {
            const ColumnNodeIDs* col = findColumn(df, "n")->as<ColumnNodeIDs>();
            for (size_t i = 0; i < col->size(); ++i) {
                actual.add({(*col)[i]});
            }
        });

        EXPECT_TRUE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_SCAN));
        EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::SCAN_NODES));
        EXPECT_TRUE(expected.equals(actual));
    }

    // === 1 hop ===
    {
        using Rows = LineContainer<NodeID>;
        Rows expected;

        std::vector<NodeID> hop1;
        expandOneHop(hop1, seeds);
        ASSERT_FALSE(hop1.empty());

        for (const NodeID nid : hop1) {
            expected.add({nid});
        }

        Rows actual;
        TestQueryInterpreter interp(&_env->getSystemManager(),
                                    _graphName,
                                    &_db->getDefaultQueryConfig());
        interp.execute(q1, [&](const Dataframe* df) {
            const ColumnNodeIDs* col = findColumn(df, "m")->as<ColumnNodeIDs>();
            for (size_t i = 0; i < col->size(); ++i) {
                actual.add({(*col)[i]});
            }
        });

        EXPECT_TRUE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_SCAN));
        EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::SCAN_NODES));
        EXPECT_TRUE(expected.equals(actual));
    }

    // === 2 hops ===
    {
        using Rows = LineContainer<NodeID>;
        Rows expected;

        std::vector<NodeID> hop1;
        expandOneHop(hop1, seeds);

        std::vector<NodeID> hop2;
        expandOneHop(hop2, hop1);
        ASSERT_FALSE(hop2.empty());

        for (const NodeID nid : hop2) {
            expected.add({nid});
        }

        Rows actual;
        TestQueryInterpreter interp(&_env->getSystemManager(),
                                    _graphName,
                                    &_db->getDefaultQueryConfig());
        interp.execute(q2, [&](const Dataframe* df) {
            const ColumnNodeIDs* col = findColumn(df, "p")->as<ColumnNodeIDs>();
            for (size_t i = 0; i < col->size(); ++i) {
                actual.add({(*col)[i]});
            }
        });

        EXPECT_TRUE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_SCAN));
        EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::SCAN_NODES));
        EXPECT_TRUE(expected.equals(actual));
    }

    // === 3 hops ===
    {
        using Rows = LineContainer<NodeID>;
        Rows expected;

        std::vector<NodeID> hop1;
        expandOneHop(hop1, seeds);

        std::vector<NodeID> hop2;
        expandOneHop(hop2, hop1);

        std::vector<NodeID> hop3;
        expandOneHop(hop3, hop2);
        ASSERT_FALSE(hop3.empty());

        for (const NodeID nid : hop3) {
            expected.add({nid});
        }

        Rows actual;
        TestQueryInterpreter interp(&_env->getSystemManager(),
                                    _graphName,
                                    &_db->getDefaultQueryConfig());
        interp.execute(q3, [&](const Dataframe* df) {
            const ColumnNodeIDs* col = findColumn(df, "q")->as<ColumnNodeIDs>();
            for (size_t i = 0; i < col->size(); ++i) {
                actual.add({(*col)[i]});
            }
        });

        EXPECT_TRUE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_SCAN));
        EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::SCAN_NODES));
        EXPECT_TRUE(expected.equals(actual));
    }
}

// A SET query with a single NodeID equality must use ConstScan.
TEST_F(ConstScanOptTest, setPropertyUsesConstScan) {
    const NodeID nid = *read().scanNodes().begin();

    const std::string q = fmt::format(
        "MATCH (n) WHERE n = {} SET n.emb = [1.0, 0.0, 0.0, 0.0]",
        nid.getValue());

    newChange();

    TestQueryInterpreter interp(&_env->getSystemManager(),
                                _graphName,
                                &_db->getDefaultQueryConfig());
    interp.execute(q, _currentChange, [](const Dataframe*) {});

    EXPECT_TRUE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_SCAN));
    EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::SCAN_NODES));
}

// When the predicate mixes NodeID equalities with a property filter,
// the optimizer must NOT rewrite to ConstScan.
TEST_F(ConstScanOptTest, mixedPredicateNoConstScan) {
    const std::string q =
        "MATCH (n) WHERE n = 0 OR n = 1 OR n.idx = 5 RETURN n";

    TestQueryInterpreter interp(&_env->getSystemManager(),
                                _graphName,
                                &_db->getDefaultQueryConfig());
    interp.execute(q, [](const Dataframe*) {});

    EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_SCAN));
    EXPECT_TRUE(interp.planHasOpcode(db::PlanGraphOpcode::SCAN_NODES));
}

// Two independent const-scan roots each followed by a 1-hop expansion.
// Verifies that each root keeps its own seed after the optimiser rewrites.
TEST_F(ConstScanOptTest, multipleRootsWithHops) {
    // Pick two Pathway seeds with different IDs.
    const LabelID pathwayLabel = read().getView().metadata().labels().get("Pathway").value();
    LabelSet pathwayLabelSet;
    pathwayLabelSet.set(pathwayLabel);
    const LabelSetHandle pathwayHandle(pathwayLabelSet);

    std::vector<NodeID> pathways;
    for (const NodeID nid : read().scanNodesByLabel(pathwayHandle)) {
        pathways.push_back(nid);
        if (pathways.size() == 2) {
            break;
        }
    }
    ASSERT_EQ(2, pathways.size());
    ASSERT_NE(pathways[0], pathways[1]);

    const NodeID seedN = pathways[0];
    const NodeID seedM = pathways[1];

    // Expected: Cartesian product of outgoing neighbours of each seed.
    std::vector<NodeID> nbrsN;
    expandOneHop(nbrsN, {seedN});
    ASSERT_FALSE(nbrsN.empty());

    std::vector<NodeID> nbrsM;
    expandOneHop(nbrsM, {seedM});
    ASSERT_FALSE(nbrsM.empty());

    using Rows = LineContainer<NodeID, NodeID>;
    Rows expected;
    for (const NodeID a : nbrsN) {
        for (const NodeID b : nbrsM) {
            expected.add({a, b});
        }
    }

    const std::string q = fmt::format(
        "MATCH (n)-->(a), (m)-->(b) WHERE n = {} AND m = {} RETURN a, b",
        seedN.getValue(), seedM.getValue());

    Rows actual;
    TestQueryInterpreter interp(&_env->getSystemManager(),
                                _graphName,
                                &_db->getDefaultQueryConfig());
    interp.execute(q, [&](const Dataframe* df) {
        auto* aCol = findColumn(df, "a")->as<ColumnNodeIDs>();
        auto* bCol = findColumn(df, "b")->as<ColumnNodeIDs>();
        ASSERT_TRUE(aCol && bCol);
        for (size_t i = 0; i < aCol->size(); ++i) {
            actual.add({aCol->at(i), bCol->at(i)});
        }
    });

    EXPECT_TRUE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_SCAN));
    EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::SCAN_NODES));
    EXPECT_TRUE(expected.equals(actual));
}

// --- ConstWriteSource optimization tests ---

// Single-variable SET with literal value triggers ConstWriteSource.
TEST_F(ConstScanOptTest, singleVarConstWriteSource) {
    auto it = read().scanNodes().begin();
    const NodeID a = *it;
    it.next();
    const NodeID b = *it;

    const std::string q = fmt::format(
        "MATCH (n) WHERE n = {} OR n = {} SET n.idx = 999",
        a.getValue(), b.getValue());

    newChange();

    TestQueryInterpreter interp(&_env->getSystemManager(),
                                _graphName,
                                &_db->getDefaultQueryConfig());
    interp.execute(q, _currentChange, [](const Dataframe*) {});

    EXPECT_TRUE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_WRITE_SOURCE));
    EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_SCAN));
    EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::CARTESIAN_PRODUCT));
}

// Multi-variable SET with same property and literal values triggers ConstWriteSource.
TEST_F(ConstScanOptTest, multiVarConstWriteSource) {
    auto it = read().scanNodes().begin();
    const NodeID a = *it;
    it.next();
    const NodeID b = *it;

    const std::string q = fmt::format(
        "MATCH (n), (m) WHERE n = {} AND m = {} SET n.idx = 10, m.idx = 20",
        a.getValue(), b.getValue());

    newChange();

    TestQueryInterpreter interp(&_env->getSystemManager(),
                                _graphName,
                                &_db->getDefaultQueryConfig());
    interp.execute(q, _currentChange, [](const Dataframe*) {});

    EXPECT_TRUE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_WRITE_SOURCE));
    EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::CARTESIAN_PRODUCT));
    EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_SCAN));
}

// Different property names across SET items must NOT trigger ConstWriteSource.
TEST_F(ConstScanOptTest, noConstWriteSourceDifferentProps) {
    auto it = read().scanNodes().begin();
    const NodeID a = *it;
    it.next();
    const NodeID b = *it;

    const std::string q = fmt::format(
        "MATCH (n), (m) WHERE n = {} AND m = {} SET n.idx = 10, m.score = 20",
        a.getValue(), b.getValue());

    newChange();

    TestQueryInterpreter interp(&_env->getSystemManager(),
                                _graphName,
                                &_db->getDefaultQueryConfig());
    interp.execute(q, _currentChange, [](const Dataframe*) {});

    EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_WRITE_SOURCE));
    EXPECT_TRUE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_SCAN));
}

// Non-literal value expression must NOT trigger ConstWriteSource.
TEST_F(ConstScanOptTest, noConstWriteSourceNonLiteral) {
    auto it = read().scanNodes().begin();
    const NodeID a = *it;
    it.next();
    const NodeID b = *it;

    // SET n.idx = m is not a literal — it references another variable.
    const std::string q = fmt::format(
        "MATCH (n), (m) WHERE n = {} AND m = {} SET n.idx = m",
        a.getValue(), b.getValue());

    newChange();

    TestQueryInterpreter interp(&_env->getSystemManager(),
                                _graphName,
                                &_db->getDefaultQueryConfig());
    interp.execute(q, _currentChange, [](const Dataframe*) {});

    EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_WRITE_SOURCE));
}

// SET with a CREATE clause present must NOT trigger ConstWriteSource.
TEST_F(ConstScanOptTest, noConstWriteSourceWithCreate) {
    const NodeID a = *read().scanNodes().begin();

    const std::string q = fmt::format(
        "MATCH (n) WHERE n = {} CREATE (:Pathway{{idx:999}}) SET n.idx = 42",
        a.getValue());

    newChange();

    TestQueryInterpreter interp(&_env->getSystemManager(),
                                _graphName,
                                &_db->getDefaultQueryConfig());
    interp.execute(q, _currentChange, [](const Dataframe*) {});

    EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_WRITE_SOURCE));
}

// When the input tree has hops (not just ConstScan→VarNode), ConstWriteSource
// must NOT fire because the tree contains non-eligible nodes.
TEST_F(ConstScanOptTest, noConstWriteSourceWithHops) {
    const NodeID a = *read().scanNodes().begin();

    const std::string q = fmt::format(
        "MATCH (n)-->(m) WHERE n = {} SET m.idx = 42",
        a.getValue());

    newChange();

    TestQueryInterpreter interp(&_env->getSystemManager(),
                                _graphName,
                                &_db->getDefaultQueryConfig());
    interp.execute(q, _currentChange, [](const Dataframe*) {});

    EXPECT_FALSE(interp.planHasOpcode(db::PlanGraphOpcode::CONST_WRITE_SOURCE));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 10;
    });
}
