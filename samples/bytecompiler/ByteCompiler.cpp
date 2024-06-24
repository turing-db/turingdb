#include <stdlib.h>
#include <assert.h>
#include <string>

#include <spdlog/spdlog.h>

#include "BytecodeGenerator.h"
#include "ASTContext.h"
#include "QueryParser.h"

using namespace db;

namespace {

QueryCommand* selectAPOE(ASTContext* ctxt) {
    QueryParser parser(ctxt);
    const std::string queryStr = "SELECT * FROM protein[APOE4]";

    QueryCommand* query = parser.parse(queryStr);
    assert(query);

    return query;
}

}

int main(int argc, char** argv) {
    // Construct query
    ASTContext ctxt;

    // Code generation
    BytecodeGenerator byteGen(selectAPOE(&ctxt));

    const bool res = byteGen.generate();
    if (!res) {
        spdlog::error("Code generation failed");
    }

    return EXIT_SUCCESS;
}
