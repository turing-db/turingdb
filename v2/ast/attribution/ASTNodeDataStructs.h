#pragma once

#include "attribution/VariableDecl.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "types/Literal.h"

namespace db {

struct PropertyExpressionData {
    const VariableDecl& _var;

    using UniquePtr = std::unique_ptr<PropertyExpressionData, void (*)(PropertyExpressionData*)>;

    static UniquePtr create(const VariableDecl& var) {
        return UniquePtr {new PropertyExpressionData {var}, &cleanUp};
    }

    static void cleanUp(PropertyExpressionData* ptr) {
        delete ptr;
    }
};

struct NodeLabelExpressionData {
    const VariableDecl& _var;
    LabelSet _labels;

    using UniquePtr = std::unique_ptr<NodeLabelExpressionData, void (*)(NodeLabelExpressionData*)>;

    static UniquePtr create(const VariableDecl& var) {
        return UniquePtr {new NodeLabelExpressionData {var}, &cleanUp};
    }

    static void cleanUp(NodeLabelExpressionData* ptr) {
        delete ptr;
    }
};

struct LiteralExpressionData {
    const Literal* _value;

    using UniquePtr = std::unique_ptr<LiteralExpressionData, void (*)(LiteralExpressionData*)>;

    static UniquePtr create(const Literal* literal) {
        return UniquePtr {new LiteralExpressionData {literal}, &cleanUp};
    }

    static void cleanUp(LiteralExpressionData* ptr) {
        delete ptr;
    }
};

struct SymbolData {
    const VariableDecl& _decl;

    using UniquePtr = std::unique_ptr<SymbolData, void (*)(SymbolData*)>;

    static UniquePtr create(const VariableDecl& decl) {
        return UniquePtr {new SymbolData {decl}, &cleanUp};
    }

    static void cleanUp(SymbolData* ptr) {
        delete ptr;
    }
};

struct NodePatternData {
    const VariableDecl& _decl;
    LabelSet _labelConstraints;
    std::vector<std::pair<PropertyType, Expression*>> _exprConstraints;

    using UniquePtr = std::unique_ptr<NodePatternData, void (*)(NodePatternData*)>;

    static UniquePtr create(const VariableDecl& decl) {
        return UniquePtr {new NodePatternData {decl}, &cleanUp};
    }

    static void cleanUp(NodePatternData* ptr) {
        delete ptr;
    }
};

struct EdgePatternData {
    const VariableDecl& _decl;
    std::vector<EdgeTypeID> _edgeTypeConstraints;
    std::vector<std::pair<PropertyType, Expression*>> _exprConstraints;

    using UniquePtr = std::unique_ptr<EdgePatternData, void (*)(EdgePatternData*)>;

    static UniquePtr create(const VariableDecl& decl) {
        return UniquePtr {new EdgePatternData {decl}, &cleanUp};
    }

    static void cleanUp(EdgePatternData* ptr) {
        delete ptr;
    }
};

}
