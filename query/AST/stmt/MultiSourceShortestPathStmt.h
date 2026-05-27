#pragma once

#include "Stmt.h"
#include "metadata/PropertyType.h"

namespace db {

class Symbol;
class VarDecl;

class MultiSourceShortestPathStmt : public Stmt {
public:
    static MultiSourceShortestPathStmt* create(CypherAST* ast,
                                               Symbol* source,
                                               Symbol* target,
                                               Symbol* edgeProperty,
                                               Symbol* sourceVar,
                                               Symbol* targetVar,
                                               Symbol* distVar,
                                               Symbol* pathVar);

    Kind getKind() const override { return Kind::MULTISOURCESHORTESTPATH; }

    Symbol* getSource() const { return _source; }
    Symbol* getTarget() const { return _target; }
    Symbol* getEdgeProperty() const { return _edgeProperty; }
    Symbol* getSourceVar() const { return _sourceVar; }
    Symbol* getTargetVar() const { return _targetVar; }
    Symbol* getDistVar() const { return _distVar; }
    Symbol* getPathVar() const { return _pathVar; }

private:
    Symbol* _source {nullptr};
    Symbol* _target {nullptr};
    Symbol* _edgeProperty {nullptr};
    Symbol* _sourceVar {nullptr};
    Symbol* _targetVar {nullptr};
    Symbol* _distVar {nullptr};
    Symbol* _pathVar {nullptr};

    MultiSourceShortestPathStmt(Symbol* source,
                                Symbol* target,
                                Symbol* edgeProperty,
                                Symbol* sourceVar,
                                Symbol* targetVar,
                                Symbol* distVar,
                                Symbol* pathVar);
    ~MultiSourceShortestPathStmt() override;
};

}
