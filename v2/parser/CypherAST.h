#pragma once

#include <memory>
#include <optional>

#include "Scope.h"

namespace db {

class CypherAST {
public:
    CypherAST() {
        _rootScope = std::make_unique<Scope>();
        _currentScope = _rootScope.get();
    }

    void beginScope() {
        _currentScope = _currentScope->newInnerScope();
    }

    void endScope() {
        _currentScope = _currentScope->getParentScope();
    }

    Scope& currentScope() {
        return *_currentScope;
    }

    void newNodePattern(std::optional<Symbol>&& symbol,
                        std::optional<std::vector<std::string>>&& labels) {
    }

private:
    std::unique_ptr<Scope> _rootScope;
    Scope* _currentScope = nullptr;
};

}
