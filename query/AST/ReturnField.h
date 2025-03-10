#pragma once

#include <string>

namespace db {

class ASTContext;
class VarDecl;

class ReturnField {
public:
    friend ASTContext;

    static ReturnField* create(ASTContext* ctxt);

    void setAll(bool isAll) { _isAll = isAll; }
    bool isAll() const { return _isAll; }

    void setName(const std::string& name) { _name = name; }
    const std::string& getName() const { return _name; }

    void setMemberName(const std::string& name) { _memberName = name; }
    const std::string& getMemberName() const { return _memberName; }

    void setDecl(VarDecl* decl) { _decl = decl; }
    VarDecl* getDecl() const { return _decl; }

private:
    bool _isAll {false};
    std::string _name;
    std::string _memberName;
    VarDecl* _decl {nullptr};

    ReturnField();
    ~ReturnField();
};

}
