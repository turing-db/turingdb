#pragma once

#include <string_view>

namespace db {

class ASTContext;
class QueryCommand;
class location;

class QueryParser {
public:
    QueryParser(ASTContext* ctxt);

    QueryCommand* parse(std::string_view query);

private:
    ASTContext* _astCtxt {nullptr};

    void generateErrorMsg(std::string& msg,
                          const std::string_view query,
                          const std::string_view excptMsg,
                          const location& loc);
};

}
