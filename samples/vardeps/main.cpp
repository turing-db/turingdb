#include <algorithm>
#include <iostream>
#include <string>

#include <argparse.hpp>
#include <spdlog/spdlog.h>
#include <utility>

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

    VariableDependencyGraph vdg;
    vdg.buildFromAST(&ast);

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
        std::vector<VariableDependencyGraphTraversal::Visit> path;
        trav.computeTraversal(&vdg, path);

        std::ranges::for_each(path, [](VariableDependencyGraphTraversal::Visit& v) {
            const std::string_view prodName = v._fstProducer ? v._fstProducer->getName() : "none";
            fmt::println("{} (discovered by {}, by means of {})", v._var->getName(), prodName, std::to_underlying(v._gen));
        });
        std::cout << '\n';
    }
}
