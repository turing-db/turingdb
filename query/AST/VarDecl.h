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
                           DeclKind kind,
                           uint64_t entityID = UINT64_MAX);

    DeclKind getKind() const { return _kind; }

    const std::string& getName() const { return _name; }

    bool isUsed() const { return _used; }
    void setUsed(bool used) { _used = used; }

    void setColumn(Column* column) { _column = column; }
    Column* getColumn() const { return _column; }

    uint64_t getEntityID() const { return _entityID; }

private:
    DeclKind _kind {DeclKind::UNKNOWN};
    std::string _name;
    bool _used {false};
    Column* _column {nullptr};
    uint64_t _entityID {UINT64_MAX};

    VarDecl(DeclKind kind, const std::string& name, uint64_t entityID);
    ~VarDecl();
};
}
