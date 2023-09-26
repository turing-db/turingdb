#include "DBServerHandler.h"

#include "Interpreter.h"

using namespace db;

void DBServerHandler::process(Buffer* outBuffer, const std::string& uri) {
    const std::string query = "list databases";
    _interp->execQuery(query, outBuffer);
}
