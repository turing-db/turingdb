#include "VariableDependencyGraphDumper.h"

#include <algorithm>
#include <cctype>
#include <ostream>
#include <string>
#include <unordered_map>

#include "VariableDependencyGraph.h"

using namespace db;

void VariableDependencyGraphDumper::dumpMermaid(const VariableDependencyGraph& graph, std::ostream& out) {
    out << "%%{init: {\"flowchart\": {\"nodeSpacing\": 40, \"rankSpacing\": 80, \"curve\": \"basis\"}}}%%\n";
    out << "flowchart LR\n";

    // Nodes whose names aren't valid Mermaid identifiers ([A-Za-z0-9_]+) need a
    // generated ID with the real name as a quoted label.
    const auto needsQuoting = [](std::string_view name) {
        if (name.empty()) {
            return true;
        }
        return !std::ranges::all_of(name, [](unsigned char c) {
            return std::isalnum(c) || c == '_';
        });
    };

    std::unordered_map<const VariableDependency*, size_t> generatedIds;
    for (size_t i {0}; const VariableDependency& var : graph.vars()) {
        if (needsQuoting(var.getName())) {
            generatedIds[&var] = i++;
        }
    }

    const auto nodeDef = [&](const VariableDependency* var) -> std::string {
        const std::string_view name = var->getName();
        if (!needsQuoting(name)) {
            return std::string(name);
        }
        const std::string id = "node" + std::to_string(generatedIds.at(var));
        if (name.empty()) {
            return id + "{\" \"}";               // diamond: anonymous merge point
        }
        return id + "(\"" + std::string(name) + "\")";  // rounded: cycle-rewrite node
    };

    for (const VariableDependency& var : graph.vars()) {
        if (var.isIsolated()) {
            out << "    " << nodeDef(&var) << "\n";
        }
    }

    for (const VariableDependency& var : graph.vars()) {
        for (const DependencyEdge* edge : var.outgoing()) {
            const EdgeMetadata::EdgeType etype = edge->data().type();
            const std::string_view typeName = EdgeTypeName::value(etype);
            const bool directed = (etype != EdgeMetadata::EdgeType::BIDIRECTIONAL);
            const std::string_view arrow = directed ? "--->" : "----";
            out << "    " << nodeDef(edge->src()) << " " << arrow << "|" << typeName << "| "
                << nodeDef(edge->tgt()) << "\n";
        }
    }
}
