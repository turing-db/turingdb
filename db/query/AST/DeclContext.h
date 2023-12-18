#pragma once

#include <unordered_map>
#include <string>
#include <vector>

namespace db {

class VarDecl;

class DeclContext {
public:
    friend VarDecl;

    DeclContext();
    ~DeclContext();

    VarDecl* getDecl(const std::string& name) const;
    
private:
    std::vector<VarDecl*> _decls;
    std::unordered_map<std::string, VarDecl*> _declMap;

    void addDecl(VarDecl* decl);
};

}
