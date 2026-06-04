#pragma once

#include <stdint.h>

#include <deque>
#include <type_traits>
#include <vector>

namespace db {

class EntityPattern;
class PatternElement;
class VarDecl;
class VariableDependency;

enum class EdgeType : uint8_t {
    OUTGOING,
    INCOMING,
    JOIN,

    _SIZE
};

class EdgeMetadata {
public:
    EdgeType type() const { return _type; }

private:
    EdgeType _type {EdgeType::_SIZE};
};

static_assert(std::is_trivially_copyable_v<EdgeMetadata>);

class DependencyEdge {
public:
    DependencyEdge(VariableDependency* tgt)
        : _tgt{tgt}
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

    void dependsOn(VariableDependency* dep);
    void requiredFor(VariableDependency* dep);

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

private:
    /// Variables whose dependencies are tracked this class
    std::deque<VariableDependency> _vars;

    VariableDependency* getOrCreateVariable(const EntityPattern* entity);
    VariableDependency* newVariable(const EntityPattern* entity);
};

}
