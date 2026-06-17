#pragma once

#include "EdgeMetadata.h"

namespace db {

class VariableDependency;
class VariableDependencyGraph;

class DependencyEdge {
public:
    DependencyEdge(VariableDependency* src, VariableDependency* tgt, EdgeMetadata data)
        : _src(src),
          _tgt(tgt),
          _data(data)
    {
    }

    VariableDependency* src() const { return _src; }
    VariableDependency* tgt() const { return _tgt; }
    EdgeMetadata data() const { return _data; }

    bool isMetaEdge() const { return _data.isMetaEdge(); }

    bool operator==(const DependencyEdge& other) const {
        return _src == other.src() && _tgt == other.tgt() && _data == other.data();
    }

private:
    friend VariableDependencyGraph;

    VariableDependency* _src {nullptr};
    VariableDependency* _tgt {nullptr};
    EdgeMetadata _data;
};

}
