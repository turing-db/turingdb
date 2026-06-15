#pragma once

#include <string>
#include <string_view>
#include <vector>

#include <range/v3/view/concat.hpp>

#include "DependencyEdge.h"

namespace db {

namespace rg = ranges;
namespace rv = rg::views;

class VariableDependencyGraph;

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

    auto edges() const { return rv::concat(_incoming, _outgoing); }
    const Edges& outgoing() const { return _outgoing; }
    const Edges& incoming() const { return _incoming; }

    std::string_view getName() const { return _name; }

    bool isIsolated() const { return edges().empty(); }

    void setName(std::string_view name) { _name = name; }

    bool isSink() const { return _outgoing.empty(); }
    bool isSource() const { return _incoming.empty(); }

    void addIncoming(DependencyEdge* newEdge);
    void addOutgoing(DependencyEdge* newEdge);

private:
    friend VariableDependencyGraph;

    Edges _incoming;
    Edges _outgoing;

    std::string _name;
};

}
