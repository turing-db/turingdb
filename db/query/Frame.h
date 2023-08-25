#pragma once

#include <vector>

#include "Value.h"
#include "Symbol.h"

namespace db::query {

class Frame {
public:
    Frame(size_t size);
    ~Frame();

    db::Value& operator[](const Symbol* sym) { return _tbl[sym->getPosition()]; }


private:
    std::vector<db::Value> _tbl;
};

}
