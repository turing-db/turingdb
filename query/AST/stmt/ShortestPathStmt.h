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

private:
    Symbol* _source {nullptr};
    Symbol* _target {nullptr};
    Symbol* _edgeProperty {nullptr};
    Symbol* _distVar {nullptr};
    Symbol* _pathVar {nullptr};

    ShortestPathStmt(Symbol* source,
                     Symbol* target,
                     Symbol* edgeProperty,
                     Symbol* distVar,
                     Symbol* pathVar);
    ~ShortestPathStmt() override;
};

}
