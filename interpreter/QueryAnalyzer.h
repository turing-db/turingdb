#pragma once

#include <stdint.h>
#include <string>

namespace db {

class ASTContext;
class QueryCommand;
class MatchCommand;
class CreateGraphCommand;
class DeclContext;
class EntityPattern;
class LoadGraphCommand;
class ExplainCommand;

class QueryAnalyzer {
public:
    QueryAnalyzer(ASTContext* ctxt);
    ~QueryAnalyzer();

    bool analyze(QueryCommand* cmd);

private:
    ASTContext* _ctxt {nullptr};
    uint64_t _nextNewVarID {0};

    bool analyzeMatch(MatchCommand* cmd);
    bool analyzeCreateGraph(CreateGraphCommand* cmd);
    bool analyzeLoadGraph(LoadGraphCommand* cmd);
    bool analyzeEntityPattern(DeclContext* declContext, EntityPattern* entity);
    bool analyzeExplain(ExplainCommand* cmd);
    std::string createVarName();
};

}
