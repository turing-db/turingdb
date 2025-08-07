#pragma once

#include "decl/VarDecl.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "types/Literal.h"

namespace db {

struct NodePatternData {
    LabelSet _labelConstraints;
    std::vector<std::pair<PropertyType, Expression*>> _exprConstraints;

    using UniquePtr = std::unique_ptr<NodePatternData, void (*)(NodePatternData*)>;

    static UniquePtr create() {
        return UniquePtr {new NodePatternData, &cleanUp};
    }

    static void cleanUp(NodePatternData* ptr) {
        delete ptr;
    }
};

struct EdgePatternData {
    std::vector<EdgeTypeID> _edgeTypeConstraints;
    std::vector<std::pair<PropertyType, Expression*>> _exprConstraints;

    using UniquePtr = std::unique_ptr<EdgePatternData, void (*)(EdgePatternData*)>;

    static UniquePtr create() {
        return UniquePtr {new EdgePatternData, &cleanUp};
    }

    static void cleanUp(EdgePatternData* ptr) {
        delete ptr;
    }
};

}
