#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <spdlog/fmt/bundled/core.h>
#include <vector>

#include "Atom.h"
#include "ParserException.h"

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

    template <IsAtom T, typename... Args>
    void createAlias(std::string&& name, Args&&... args) {
        if (_names.contains(name)) {
            throw ParserException(fmt::format("Alias '{}' already exists", name));
        }

        _names.emplace(std::move(name), std::make_shared<T>(std::forward<Args>(args)...));
    }

    void renameAlias(const std::string& oldName, std::string&& newName) {
        auto it = _names.find(oldName);

        if (it == _names.end()) {
            throw ParserException(fmt::format("Variable '{}' not defined", oldName));
        }

        _names.erase(it);
        _names.emplace(std::move(newName), it->second);
    }

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
    std::unordered_map<std::string, std::shared_ptr<Atom>> _names;
    Scope* _parentScope = nullptr;
    std::vector<std::unique_ptr<Scope>> _innerScopes;
};

}
