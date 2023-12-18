#pragma once

namespace db {

class ASTContext;
class QueryCommand;
class SelectCommand;
class DeclContext;
class EntityPattern;

class QueryAnalyzer {
public:
    QueryAnalyzer(ASTContext* ctxt);
    ~QueryAnalyzer();

    bool analyze(QueryCommand* cmd);

private:
    ASTContext* _ctxt {nullptr};

    bool analyzeSelect(SelectCommand* cmd);
    
    template <class Pattern>
    bool analyzeNodeOrEdgePattern(DeclContext* declContext, Pattern* pattern);

    bool analyzeEntityPattern(DeclContext* declContext, EntityPattern* entity);
};

}