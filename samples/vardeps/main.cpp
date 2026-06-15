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
#include "versioning/Transaction.h"

#include "VariableDependencyGraph.h"

#include "IRDumper.h"


#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/StmtContainer.h"

#include "ToolInit.h"

/**

problematic queries

1. (x)-[e]->(a), (x)<-[e]-(a)
Will always return nothing (e cannot be both an in edge and an out
edge of x), however it causes the dep graph to be non-simple (multigraph),
and causes the cycle basis algorithm to report 2 cycles.

2. (x)-->(x), (x)<--(x)
Verify the generated graph is correct

3. (y)<--(e)<--(x)

*/

using namespace db;

static void inspectVarDepGraph(CypherAST* ast) {
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
    IRDumper::dumpMermaid(vdg, std::cout);

    const auto printCycle = [](auto& c) {
        for (auto* v : c) {
            fmt::print("{} ", v->getName());
        }
        fmt::print("\n");
    };

    const auto basis = [&]() {
        auto cycs = vdg.cycleBasis();
        for (auto& c : cycs) {
            printCycle(c);
            vdg.canonicaliseCycle(c);
            spdlog::warn("CANONICALISED:");
            printCycle(c);
            fmt::println("");
            vdg.detachCycle(c);
            IRDumper::dumpMermaid(vdg, std::cout);
        }
    };

    basis();
    IRDumper::dumpMermaid(vdg, std::cout);
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

    inspectVarDepGraph(&ast);
}
