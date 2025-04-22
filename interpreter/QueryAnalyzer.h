#pragma once

#include <stdint.h>
#include <string>

namespace db {

class PropertyTypeMap;
class ASTContext;
class QueryCommand;
class MatchCommand;
class CreateCommand;
class CreateGraphCommand;
class DeclContext;
class EntityPattern;
class LoadGraphCommand;
class ExplainCommand;

class QueryAnalyzer {
public:
    QueryAnalyzer(ASTContext* ctxt, const PropertyTypeMap& propTypeMap);
    ~QueryAnalyzer();

    bool analyze(QueryCommand* cmd);

private:
    ASTContext* _ctxt {nullptr};
    const PropertyTypeMap& _propTypeMap;
    uint64_t _nextNewVarID {0};

    bool analyzeMatch(MatchCommand* cmd);
    bool analyzeCreate(CreateCommand* cmd);
    bool analyzeCreateGraph(CreateGraphCommand* cmd);
    bool analyzeLoadGraph(LoadGraphCommand* cmd);
    bool analyzeEntityPattern(DeclContext* declContext, EntityPattern* entity);
    bool analyzeExplain(ExplainCommand* cmd);
    std::string createVarName();
};

}
