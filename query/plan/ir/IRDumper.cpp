#include "IRDumper.h"

#include <ostream>
#include <string>
#include <unordered_map>

#include "VariableDependencyGraph.h"
#include "decl/VarDecl.h"

using namespace db;

void IRDumper::dumpMermaid(const VariableDependencyGraph& graph, std::ostream& out) {
    out << "flowchart TD\n";

    std::unordered_map<const VariableDependency*, size_t> nodeIds;
    for (size_t i {0}; const VariableDependency& var : graph) {
        nodeIds[&var] = i++;
    }

    const auto nodeDef = [&](const VariableDependency* var) {
        const std::string_view name = var->getDecl()->getName();
        const std::string_view label = name.empty() ? "<unnamed>" : name;
        return "v" + std::to_string(nodeIds.at(var)) + "[\"" + std::string(label) + "\"]";
    };

    for (const VariableDependency& var : graph) {
        if (var.isIsolated()) {
            out << "    " << nodeDef(&var) << "\n";
        } else {
            for (const DependencyEdge& edge : var.getOutgoing()) {
                out << "    " << nodeDef(&var) << " --> " << nodeDef(edge.tgt()) << "\n";
            }
        }
    }
}
