#pragma once

#include <string>

#include "DeclKind.h"

namespace db {

class ASTContext;
class DeclContext;
class Column;

class VarDecl {
public:
    friend ASTContext;

    static VarDecl* create(ASTContext* astCtxt,
                           DeclContext* declContext,
                           const std::string& name,
                           DeclKind kind);

    DeclKind getKind() const { return _kind; }

    const std::string& getName() const { return _name; }

    bool isReturned() const { return _returned; }

    void setReturned(bool returned) { _returned = returned; }

    void setColumn(Column* column) { _column = column; }

    Column* getColumn() const { return _column; }

private:
    DeclKind _kind {DeclKind::UNKNOWN};
    std::string _name;
    bool _returned {false};
    Column* _column {nullptr};

    VarDecl(DeclKind kind, const std::string& name);
    ~VarDecl();
};
}
