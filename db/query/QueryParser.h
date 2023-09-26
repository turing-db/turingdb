#pragma once

#include <string>

#include "ASTContext.h"

namespace db {

class QueryCommand;

class QueryParser {
public:
    QueryParser();

    QueryCommand* parse(const std::string& queryStr);

private:
    ASTContext _astCtxt;
};

}
