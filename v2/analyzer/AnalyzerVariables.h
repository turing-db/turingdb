#pragma once

#include <string_view>

#include "decl/EvaluatedType.h"

namespace db::v2 {

class CypherAST;
class VarDecl;
class DeclContext;

class AnalyzerVariables {
public:
    explicit AnalyzerVariables(CypherAST* ast);
    ~AnalyzerVariables();

    AnalyzerVariables(const AnalyzerVariables&) = delete;
    AnalyzerVariables(AnalyzerVariables&&) = delete;
    AnalyzerVariables& operator=(const AnalyzerVariables&) = delete;
    AnalyzerVariables& operator=(AnalyzerVariables&&) = delete;

    void setContext(DeclContext* ctxt) { _ctxt = ctxt; }

    VarDecl* getDecl(std::string_view name) const;
    VarDecl* getOrCreateNamedVariable(EvaluatedType type, std::string_view name);
    VarDecl* createUnnamedVariable(EvaluatedType type);

private:
    CypherAST* _ast {nullptr};
    DeclContext* _ctxt {nullptr};
    uint64_t _unnamedVarCounter {0};

    void throwError(std::string_view msg, const void* obj = 0) const;
};

}
