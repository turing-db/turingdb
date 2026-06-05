#pragma once

#include <stdint.h>

#include <deque>
#include <type_traits>
#include <vector>

#include "EnumToString.h"

namespace db {

class EntityPattern;
class PatternElement;
class VarDecl;
class VariableDependency;

class EdgeMetadata {
public:
    enum class EdgeType : uint8_t;

    explicit EdgeMetadata(EdgeType type)
        : _type(type)
    {
    }

    enum class EdgeType : uint8_t {
        OUTGOING,
        INCOMING,
        BIDIRECTIONAL,
        MERGE,

        _SIZE
    };

    EdgeType type() const { return _type; }

    static bool isMetaEdge(EdgeType et) { return et == EdgeType::MERGE; }

private:
    EdgeType _type {EdgeType::_SIZE};
};

static_assert(std::is_trivially_copyable_v<EdgeMetadata>);


using EdgeTypeName = EnumToString<EdgeMetadata::EdgeType>::Create<
    EnumStringPair<EdgeMetadata::EdgeType::OUTGOING, "getout">,
    EnumStringPair<EdgeMetadata::EdgeType::INCOMING, "getin">,
    EnumStringPair<EdgeMetadata::EdgeType::BIDIRECTIONAL, "bidir">,
    EnumStringPair<EdgeMetadata::EdgeType::MERGE, "merge">
>;

class DependencyEdge {
public:
    DependencyEdge(VariableDependency* tgt, EdgeMetadata data)
        : _tgt(tgt),
        _data(data)
    {
    }

    const VariableDependency* tgt() const { return _tgt; }
    EdgeMetadata data() const { return _data; }

private:
    VariableDependency* _tgt;
    EdgeMetadata _data;
};

/**
 * @brief A representation of a variable and its dependencies.
 * @detail Used in @ref VariableDependencyGraph
 */
class VariableDependency {
public:
    using Edges = std::vector<DependencyEdge>;

    VariableDependency(VarDecl* decl)
        : _decl(decl)
    {
    }

    void dependsOn(VariableDependency* dep, EdgeMetadata::EdgeType type);
    void requiredFor(VariableDependency* dep, EdgeMetadata::EdgeType type);

    VarDecl* getDecl() const { return _decl; }
    const Edges& getOutgoing() const { return _outgoing; }
    const Edges& getIncoming() const { return _incoming; }

    bool isRoot() const { return _incoming.empty(); }
    bool isSink() const { return _outgoing.empty(); }
    bool isIsolated() const { return isRoot() && isSink(); }

private:
    VarDecl* _decl {nullptr};

    Edges _incoming;
    Edges _outgoing;
};

/**
 * @brief Graph representation of the dependencies among variables, generated from
 * an analyzed AST.
 * @warn Ensure any AST elements passed to this class have been appropriately analysed, to
 * populate @ref VarDecl s.
 */
class VariableDependencyGraph {
public:
    /// Given a pattern (e.g. (n)-[e]->(m)), inserts all vars into the dependency graph
    void registerPatternElement(const PatternElement* ptn);

    /// Iteration order has no semantic meaning
    auto begin() const { return std::cbegin(_vars); }
    auto end() const { return std::cend(_vars); }

    void forestify();

private:
    /// Variables whose dependencies are tracked this class
    std::deque<VariableDependency> _vars;

    VariableDependency* getOrCreateVariable(const EntityPattern* entity);
    VariableDependency* newVariable(const EntityPattern* entity);
};

}
