#include "IRDumper.h"

#include <ostream>
#include <string>
#include <unordered_map>

#include "VariableDependencyGraph.h"

using namespace db;

void IRDumper::dumpMermaid(const VariableDependencyGraph& graph, std::ostream& out) {
    out << "flowchart TD\n";

    // Unnamed nodes (synthetic merge points) need a generated ID; named nodes use
    // their variable name directly as the Mermaid ID so the script shows it as-is.
    std::unordered_map<const VariableDependency*, size_t> anonIds;
    for (size_t i {0}; const VariableDependency& var : graph.vars()) {
        if (var.getName().empty()) {
            anonIds[&var] = i++;
        }
    }

    const auto nodeDef = [&](const VariableDependency* var) -> std::string {
        const std::string_view name = var->getName();
        if (!name.empty()) {
            return std::string(name);
        }
        return "anon" + std::to_string(anonIds.at(var)) + "[\"<unnamed>\"]";
    };

    for (const VariableDependency& var : graph.vars()) {
        if (var.isIsolated()) {
            out << "    " << nodeDef(&var) << "\n";
        }
    }

    for (const DependencyEdge& edge : graph.edges()) {
        const EdgeMetadata::EdgeType etype = edge.data().type();
        const std::string_view typeName = EdgeTypeName::value(etype);
        out << "    " << nodeDef(edge.u()) << " ---|" << typeName << "| "
            << nodeDef(edge.v()) << "\n";
    }
}
