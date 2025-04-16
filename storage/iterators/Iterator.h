#pragma once

#include "views/GraphView.h"
#include "PartIterator.h"

namespace db {

class Iterator {
public:
    Iterator() = default;

    explicit Iterator(const GraphView& view)
        : _partIt(view),
        _view(view)
    {
    }

    Iterator(const Iterator& other) = default;
    Iterator(Iterator&&) = default;
    Iterator& operator=(const Iterator& other) = default;
    Iterator& operator=(Iterator&&) = default;
    virtual ~Iterator() = default;

    virtual void next() = 0;

    virtual bool isValid() const {
        return _partIt.isValid();
    }

    const PartIterator& getPartIterator() const {
        return _partIt;
    }

    virtual bool operator!=(const DataPartIterator& other) const {
        return other != _partIt.getIterator();
    }

protected:
    PartIterator _partIt;
    GraphView _view;

    void reset() {
        _partIt = PartIterator(_view);
    }
};

}
