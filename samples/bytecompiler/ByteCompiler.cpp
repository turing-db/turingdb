#include <stdlib.h>
#include <string>

#include <spdlog/spdlog.h>
#include <argparse.hpp>

#include "BytecodeCompiler.h"
#include "ASTContext.h"
#include "QueryParser.h"
#include "QueryAnalyzer.h"

#include "ToolInit.h"
#include "Panic.h"

using namespace db;

namespace {

/*
QueryCommand* selectAPOE(ASTContext* ctxt) {
    QueryParser parser(ctxt);
    const std::string queryStr = "SELECT * FROM protein[APOE4]";

    QueryCommand* query = parser.parse(queryStr);
    assert(query);

    return query;
}
*/

QueryCommand* selectGenesAndProteins(ASTContext* ctxt) {
    QueryParser parser(ctxt);
    const std::string queryStr = "SELECT p,g FROM (p:protein)--(g:gene)";

    QueryCommand* query = parser.parse(queryStr);
    if (!query) {
        panic("Syntax error in the query");
    }

    QueryAnalyzer analyzer(ctxt, query);
    if (!analyzer.analyze()) {
        panic("Query analysis failed");
    }

    return query;
}

QueryCommand* parseQuery(ASTContext* ctxt, const std::string queryStr) {
    QueryParser parser(ctxt);

    QueryCommand* query = parser.parse(queryStr);
    if (!query) {
        panic("Syntax error in the query");
    }

    QueryAnalyzer analyzer(ctxt, query);
    if (!analyzer.analyze()) {
        panic("Query analysis failed");
    }

    return query;
}

}

int main(int argc, const char** argv) {
    ToolInit toolInit("bytecodecompiler");
    toolInit.disableOutputDir();

    std::string queryStr;

    auto& argparser = toolInit.getArgParser();
    argparser.add_argument("-i")
             .help("Compile query given as argument")
             .nargs(1)
             .metavar("query")
             .store_into(queryStr);

    toolInit.init(argc, argv);

    try {
        // Construct query
        ASTContext ctxt;

        QueryCommand* queryCmd = queryStr.empty() ?
            selectGenesAndProteins(&ctxt) 
            : parseQuery(&ctxt, queryStr);

        // Code generation
        BytecodeCompiler byteGen(queryCmd);

        if (!byteGen.generate()) {
            spdlog::error("Bytecode generation failed");
            return EXIT_FAILURE;
        }
    } catch (const TuringException& e) {
        spdlog::error("Exception: {}", e.what());
        spdlog::error("Bytecode compilation failed");
    }

    return EXIT_SUCCESS;
}
