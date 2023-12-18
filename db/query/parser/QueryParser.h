#pragma once

#include "StringSpan.h"

namespace db {

class ASTContext;
class QueryCommand;

class QueryParser {
public:
    QueryParser(ASTContext* ctxt);

    QueryCommand* parse(StringSpan query);

private:
    ASTContext* _astCtxt {nullptr};
};

}
