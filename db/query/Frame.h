#pragma once

#include <vector>

#include "Value.h"
#include "Symbol.h"

namespace db {

class SymbolTable;

class Frame {
public:
    Frame(SymbolTable* symTbl);
    ~Frame();

    db::Value& operator[](const Symbol* sym) { return _tbl[sym->getPosition()]; }

private:
    SymbolTable* _symTbl {nullptr};
    std::vector<db::Value> _tbl;
};

}
