#pragma once

#include "views/GraphView.h"

namespace db {

class Column;

class Index {
public:
    Index() = default;
    virtual ~Index() = default;

    Index(const Index&) = delete;
    Index(Index&&) = delete;
    Index& operator=(const Index&) = delete;
    Index& operator=(Index&&) = delete;

    virtual void init(GraphView view) = 0;
    virtual void query(const Column* query, Column* result) = 0;

    std::string_view name() const { return _name; }

    // MAYBE: DataParts which it indexes

protected:
    std::string _name;
    bool _initialised {false};
};

}
