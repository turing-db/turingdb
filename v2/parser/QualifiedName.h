#pragma once

#include <vector>

#include "Symbol.h"

namespace db {

class QualifiedName {
public:
    QualifiedName() = default;
    ~QualifiedName() = default;

    QualifiedName(const QualifiedName&) = default;
    QualifiedName(QualifiedName&&) = default;
    QualifiedName& operator=(const QualifiedName&) = default;
    QualifiedName& operator=(QualifiedName&&) = default;

    void addSymbol(Symbol&& symbol) { _symbols.emplace_back(std::move(symbol)); }

    const std::vector<Symbol>& getSymbols() const { return _symbols; }

private:
    std::vector<Symbol> _symbols;
};

}
