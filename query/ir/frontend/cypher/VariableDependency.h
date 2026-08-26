#pragma once

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <range/v3/view/concat.hpp>

#include "DependencyEdge.h"

namespace db {

namespace rg = ranges;
namespace rv = rg::views;

class VarDecl;
class VariableDependencyGraph;

/**
 * @brief A representation of a variable and its dependencies.
 * @detail Used in @ref VariableDependencyGraph
 */
class VariableDependency {
public:
    using Edges = std::vector<DependencyEdge*>;

    using LabelNames = std::vector<std::string_view>;
    using EdgeType = std::string_view;
    using Constraint = std::variant<LabelNames, EdgeType>;

    explicit VariableDependency(std::string_view name)
        : _name(name)
    {
    }

    VariableDependency(std::string_view name, const VarDecl* decl)
        : _name(name),
        _decl(decl)
    {
    }

    auto edges() const { return rv::concat(_incoming, _outgoing); }
    const Edges& outgoing() const { return _outgoing; }
    const Edges& incoming() const { return _incoming; }

    std::string_view getName() const { return _name; }
    const VarDecl* getDecl() const { return _decl; }

    const std::optional<Constraint>& constraints() const { return _constraints; }

    bool isIsolated() const { return edges().empty(); }

    void setName(std::string_view name) { _name = name; }

    bool isSink() const { return _outgoing.empty(); }
    bool isSource() const { return _incoming.empty(); }

    void addIncoming(DependencyEdge* newEdge);
    void addOutgoing(DependencyEdge* newEdge);

    void addLabelConstraints(std::span<const std::string_view> labels);
    void setEdgeTypeConstraint(std::string_view type);

private:
    friend VariableDependencyGraph;

    Edges _incoming;
    Edges _outgoing;

    std::optional<Constraint> _constraints;

    std::string _name;
    const VarDecl* _decl {nullptr};
};

}
