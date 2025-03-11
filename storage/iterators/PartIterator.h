#pragma once

#include <memory>

#include "views/GraphView.h"
#include "DataPartSpan.h"

namespace db {

class PartIterator {
public:
    PartIterator() = default;
    explicit PartIterator(const GraphView& view);

    PartIterator(const PartIterator& other) = default;
    PartIterator(PartIterator&&) = default;
    PartIterator& operator=(const PartIterator& other) = default;
    PartIterator& operator=(PartIterator&&) = default;
    ~PartIterator() = default;

    inline const DataPart* get() const { return _it->get(); }
    inline DataPartIterator getIterator() const { return _it; }
    inline DataPartIterator getEndIterator() const { return _itEnd; }

    inline void next() { ++_it; }

    bool isValid() const {
        return _it != _itEnd;
    }

    bool operator!=(const PartIterator& other) const {
        return _it != other._it;
    }

    inline PartIterator& operator++() {
        ++_it;
        return *this;
    }

    void skipEmptyParts();

    void skipToEnd() {
        _it = _itEnd;
    }

private:
    DataPartIterator _it;
    DataPartIterator _itEnd;
};

}
