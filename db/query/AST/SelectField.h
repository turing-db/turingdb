#pragma once

#include <string>

namespace db {

class ASTContext;
class VarDecl;

class SelectField {
public:
    friend ASTContext;

    static SelectField* create(ASTContext* ctxt);

    void setAll(bool isAll) { _isAll = isAll; }
    bool isAll() const { return _isAll; }

    void setName(const std::string& name) { _name = name; }
    const std::string& getName() const { return _name; }

    void setDecl(VarDecl* decl) { _decl = decl; }
    VarDecl* getDecl() const { return _decl; }

private:
    bool _isAll {false};
    std::string _name;
    VarDecl* _decl {nullptr};

    SelectField();
    ~SelectField();
};

}
