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
    virtual const Column* query(const Column* input) = 0;

    // MAYBE: DataParts which it indexes
};

}
