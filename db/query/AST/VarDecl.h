#pragma once

#include <string>

namespace db {

class ASTContext;
class DeclContext;

class VarDecl {
public:
    friend ASTContext;

    static VarDecl* create(ASTContext* astCtxt,
                           DeclContext* declContext,
                           const std::string& name);

    const std::string& getName() const { return _name; }

private:
    std::string _name;

    VarDecl(const std::string& name);
    ~VarDecl();
};

}
