#include <algorithm>
#include <iostream>
#include <string>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "SimpleGraph.h"
#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "VariableDependency.h"
#include "VariableDependencyGraphTraversal.h"
#include "versioning/Transaction.h"

#include "VariableDependencyGraph.h"

#include "VariableDependencyGraphDumper.h"


#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/StmtContainer.h"

#include "ToolInit.h"

using namespace db;

static VariableDependencyGraph getVDG(CypherAST* ast) {
    bioassert(ast->queries().size() == 1, "Single queries only.");
    const QueryCommand* q = ast->queries().front();
    const auto* spq = dynamic_cast<const SinglePartQuery*>(q);

    const StmtContainer* stmtsContainer = spq->getReadStmts();
    const StmtContainer::Stmts& stmts = stmtsContainer->stmts();

    VariableDependencyGraph vdg;
    for (Stmt* s : stmts) {
        const auto* rd = dynamic_cast<const MatchStmt*>(s);
        if (!rd) {
            spdlog::warn("Non-match statement: skipped");
            continue;
        }

        const Pattern* ptn = rd->getPattern();
        const Pattern::PatternElements& eles = ptn->elements();
        for (const PatternElement* ele : eles) {
            vdg.registerPatternElement(ele);
        }
    }

    return vdg;
}

int main(int argc, const char** argv) {
    ToolInit toolInit("ir");
    toolInit.disableOutputDir();

    std::string query = "MATCH (n) RETURN n";

    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("-q", "--query")
             .metavar("cypher")
             .store_into(query)
             .help("Cypher query to parse (default: MATCH (n) RETURN n)");

    toolInit.init(argc, argv);

    TuringConfig config;
    config.setSyncedOnDisk(false);

    SystemManager sysman(&config);
    sysman.init();

    SystemAccessor acc = sysman.accessUnique();

    Graph* graph = acc.createGraph("simpledb");
    SimpleGraph::createSimpleGraph(graph);

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    CypherAST ast(acc.getProcedures(), query);

    {
        CypherParser parser(&ast);
        parser.parse(query);
        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();
    }

    VariableDependencyGraph vdg = getVDG(&ast);
    VariableDependencyGraphDumper::dumpMermaid(vdg, std::cout);

    vdg.eliminateCycles();
    VariableDependencyGraphDumper::dumpMermaid(vdg, std::cout);

    VariableDependencyGraphTraversal trav;
    {
        std::vector<const VariableDependency*> path;
        trav.getTraversal(&vdg, path);

        std::ranges::for_each(path, [](auto&& v) { std::cout << v->getName() << " "; });
        std::cout << '\n';
    }
    {
        std::vector<const DependencyEdge*> path;
        trav.edgeTraversal(&vdg, path);

        std::ranges::for_each(path, [](auto&& e) {
            std::cout << e->src()->getName() << " -> " << e->tgt()->getName() << ", ";
        });
        std::cout << '\n';
    }
}
