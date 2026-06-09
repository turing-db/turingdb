#pragma once

#include <cstdint>
#include <stdint.h>

#include <deque>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "EnumToString.h"

namespace db {

class EntityPattern;
class PatternElement;
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

    bool operator==(const EdgeMetadata& other) const {
        return _type == other._type;
    }

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
    DependencyEdge(VariableDependency* src, VariableDependency* tgt, EdgeMetadata data)
        : _src(src),
         _tgt(tgt),
        _data(data)
    {
    }

    const VariableDependency* src() const { return _src; }
    const VariableDependency* tgt() const { return _tgt; }
    EdgeMetadata data() const { return _data; }

    bool operator==(const DependencyEdge& other) const {
        return _src == other.src() && _tgt == other.tgt() && _data == other.data();
    }

private:
    friend class VariableDependencyGraph;

    VariableDependency* _src;
    VariableDependency* _tgt;
    EdgeMetadata _data;
};

/**
 * @brief A representation of a variable and its dependencies.
 * @detail Used in @ref VariableDependencyGraph
 */
class VariableDependency {
public:
    using Edges = std::vector<DependencyEdge*>;

    explicit VariableDependency(std::string_view name)
        : _name(name)
    {
    }

    auto edges() const;

    std::string_view getName() const { return _name; }

    bool isIsolated() const;

    void setName(std::string_view name) { _name = name; }

    bool isSink() const { return _outgoing.empty(); }
    bool isSource() const { return _incoming.empty(); }

    const Edges& outgoing() const { return _outgoing; }
    const Edges& incoming() const { return _incoming; }

    void addIncoming(DependencyEdge* newEdge);
    void addOutgoing(DependencyEdge* newEdge);

private:
    friend class VariableDependencyGraph;

    std::string _name;

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
    using Cycle = std::vector<VariableDependency*>;
    
    /// Given a pattern (e.g. (n)-[e]->(m)), inserts all vars into the dependency graph
    void registerPatternElement(const PatternElement* ptn);

    /// Iteration order has no semantic meaning
    const auto& vars() const { return _vars; }
    const auto& edges() const { return _edges; }

    const DependencyEdge* addDirected(VariableDependency* src, VariableDependency* tgt, const EdgeMetadata& data);

    Cycle getCycle();

    std::vector<Cycle> cycleBasis();
    std::vector<Cycle> paton();

    void rewriteCycle(const Cycle& cyc);

    void detachCycle(const Cycle& cyc);

    Cycle _getCycle();

    void applyMerges();

private:
    /// Variables whose dependencies are tracked by this class
    std::deque<VariableDependency> _vars;
    std::deque<DependencyEdge> _edges;

    std::unordered_map<VariableDependency*, int> _anonymised;

    std::unordered_map<VariableDependency*,
                       std::pair<VariableDependency*, VariableDependency*>>
        _anonMap;

    using Visited = std::unordered_map<VariableDependency*, uint8_t>;
    using Parents = std::unordered_map<VariableDependency*, VariableDependency*>;
    Visited _cyclicVisited;
    Parents _cyclicParents;

    VariableDependency* getOrCreateVariable(const EntityPattern* entity);
    VariableDependency* newVariable(const EntityPattern* entity);
    VariableDependency* newVariable(std::string_view name);

    Cycle findCycle(VariableDependency* curr, VariableDependency* prev,
                    std::unordered_set<const VariableDependency*>& visited,
                    std::vector<VariableDependency*>& path,
                    std::unordered_set<const VariableDependency*>& pathSet);

    VariableDependency* _dfs(VariableDependency* u, VariableDependency* par);

    std::string getNextAnonymisation(VariableDependency* v);

    void addMerge(VariableDependency* from1, VariableDependency* from2,
                  VariableDependency* into, VariableDependency* via1,
                  VariableDependency* via2);

    void resetCycleState();

    static bool patchEdgeSrc(DependencyEdge* e,
                             VariableDependency* oldSrc,
                             VariableDependency* newSrc);
    static bool patchEdgeTgt(DependencyEdge* e,
                             VariableDependency* oldtgt,
                             VariableDependency* newTgt);

    void addBetween(VariableDependency* s, VariableDependency* mid, VariableDependency* t);

    void addBetweenOutImpl(VariableDependency* s, VariableDependency* mid, VariableDependency* t, DependencyEdge* e);
    void addBetweenIncImpl(VariableDependency* s, VariableDependency* mid, VariableDependency* t, DependencyEdge* e);
};

}
