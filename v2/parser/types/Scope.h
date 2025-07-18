#pragma once

#include <memory>
#include <vector>

namespace db {

class Scope {
public:
    Scope() = default;
    ~Scope() = default;

    Scope(Scope* parentScope)
        : _parentScope(parentScope) {
    }

    Scope(Scope&&) = delete;
    Scope(const Scope&) = delete;
    Scope& operator=(const Scope&) = delete;
    Scope& operator=(Scope&&) = delete;

    Scope* getParentScope() const {
        return _parentScope;
    }

    Scope* newInnerScope() {
        auto newScope = std::make_unique<Scope>(this);
        auto* ptr = newScope.get();
        _innerScopes.emplace_back(std::move(newScope));

        return ptr;
    }

private:
    Scope* _parentScope = nullptr;
    std::vector<std::unique_ptr<Scope>> _innerScopes;
};

}
