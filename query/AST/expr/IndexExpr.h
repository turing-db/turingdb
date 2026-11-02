#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class VarDecl;

class IndexExpr : public Expr {
public:
    static IndexExpr* create(CypherAST* ast, Expr* base, Expr* indexExpr);

    Expr* getBase() const { return _base; }
    Expr* getIndexExpr() const { return _indexExpr; }

    bool hasLiteralIndex() const { return _hasLiteralIndex; }
    size_t getLiteralIndex() const { return _literalIndex; }

    bool readsAMapKey() const { return _readsAMapKey; }
    void setReadsAMapKey();

    // The field of the loaded row this access reads, under the declaration the load
    // publishes its column with. Null when the access names no field of a load, which a
    // computed index does. Kept apart from the expression's own declaration, since an
    // alias on the item replaces that one.
    VarDecl* getCSVFieldDecl() const { return _csvFieldDecl; }

    // m.key for m['key'] over a map, an entity or a loaded row, where the bracket reads what
    // the dotted form reads. Null wherever the bracket reads on its own.
    Expr* getDottedForm() const { return _dottedForm; }

    void setLiteralIndex(size_t index) {
        _hasLiteralIndex = true;
        _literalIndex = index;
    }

    void setCSVFieldDecl(VarDecl* decl) { _csvFieldDecl = decl; }
    void setDottedForm(Expr* dottedForm) { _dottedForm = dottedForm; }

private:
    Expr* _base {nullptr};
    Expr* _indexExpr {nullptr};
    VarDecl* _csvFieldDecl {nullptr};
    Expr* _dottedForm {nullptr};
    bool _hasLiteralIndex {false};
    bool _readsAMapKey {false};
    size_t _literalIndex {0};

    IndexExpr(Expr* base, Expr* indexExpr);

    ~IndexExpr() override;
};

}
