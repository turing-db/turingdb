#include <iostream>
#include <string>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "CompilerException.h"
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

#include "ToolInit.h"
#include "ir/VariableDependencyGraph.h"
#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/StmtContainer.h"

using namespace db;

void inspectVarDepGraph(CypherAST* ast) {
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
    vdg.dumpMermaid(std::cout);
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

    Graph* graph = sysman.createGraph("ir");
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    CypherAST ast(sysman.getProcedures(), query);

    {
        CypherParser parser(&ast);
        try {
            parser.parse(query);
        } catch (const CompilerException& e) {
            spdlog::error("Parse error: {}", e.what());
            return EXIT_FAILURE;
        }
    }

    {
        CypherAnalyzer analyzer(&ast, view);
        try {
            analyzer.analyze();
        } catch (const CompilerException& e) {
            spdlog::error("Analysis error: {}", e.what());
            return EXIT_FAILURE;
        }
    }

    inspectVarDepGraph(&ast);

    return EXIT_SUCCESS;
}
