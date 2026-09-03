#pragma once

#include "Stmt.h"
#include "metadata/PropertyType.h"

namespace db {

class Symbol;
class VarDecl;

class ShortestPathStmt : public Stmt {
public:
    static ShortestPathStmt* create(CypherAST* ast,
                                    Symbol* source,
                                    Symbol* target,
                                    Symbol* edgeProperty,
                                    Symbol* distVar,
                                    Symbol* pathVar);

    Kind getKind() const override { return Kind::SHORTESTPATH; }

    Symbol* getSource() const { return _source; }
    Symbol* getTarget() const { return _target; }
    Symbol* getEdgeProperty() const { return _edgeProperty; }
    Symbol* getDistVar() const { return _distVar; }
    Symbol* getPathVar() const { return _pathVar; }

    VarDecl* getSourceDecl() const { return _sourceDecl; }
    VarDecl* getTargetDecl() const { return _targetDecl; }
    VarDecl* getDistDecl() const { return _distDecl; }
    VarDecl* getPathDecl() const { return _pathDecl; }

    void setSourceDecl(VarDecl* decl) { _sourceDecl = decl; }
    void setTargetDecl(VarDecl* decl) { _targetDecl = decl; }
    void setDistDecl(VarDecl* decl) { _distDecl = decl; }
    void setPathDecl(VarDecl* decl) { _pathDecl = decl; }

private:
    Symbol* _source {nullptr};
    Symbol* _target {nullptr};
    Symbol* _edgeProperty {nullptr};
    Symbol* _distVar {nullptr};
    Symbol* _pathVar {nullptr};
    VarDecl* _sourceDecl {nullptr};
    VarDecl* _targetDecl {nullptr};
    VarDecl* _distDecl {nullptr};
    VarDecl* _pathDecl {nullptr};

    ShortestPathStmt(Symbol* source,
                     Symbol* target,
                     Symbol* edgeProperty,
                     Symbol* distVar,
                     Symbol* pathVar);
    ~ShortestPathStmt() override;
};

}
