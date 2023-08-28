#pragma once

#include <string>

class Cell;

namespace turing::db::client {

class QueryValue {
public:
    QueryValue(const Cell& cell)
        : _cell(cell)
    {
    }

    std::string toString() const;

private:
    const Cell& _cell;
};

}
