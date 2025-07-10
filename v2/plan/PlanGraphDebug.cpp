#include "PlanGraphDebug.h"

#include "BioAssert.h"
#include "VarDecl.h"
#include "views/GraphView.h"
#include "PlanGraph.h"
#include "Expr.h"

using namespace db;

void PlanGraphDebug::dumpMermaid(std::ostream& output, const GraphView& view, const PlanGraph& planGraph) {
    const auto& metadata = view.metadata();
    const auto& edgeTypeMap = metadata.edgeTypes();
    const auto& labelMap = metadata.labels();

    output << "---\n";
    output << "config:\n";
    output << "  layout: dagre\n";
    output << "---\n";
    output << "erDiagram\n";

    for (const auto& node : planGraph._nodes) {
        // Writing node definition
        output << fmt::format("    {} {{\n", fmt::ptr(node.get()));
        output << fmt::format("        opcode {}\n", PlanGraphOpcodeDescription::value(node->getOpcode()));

        if (const auto* n = dynamic_cast<VarNode*>(node.get())) {
            bioassert(n->getVarDecl());
            output << fmt::format("        name _{}\n", n->getVarDecl()->getName());
        }

        else if (const auto* n = dynamic_cast<FilterEdgeTypeNode*>(node.get())) {
            output << fmt::format("        edge_type {}\n", edgeTypeMap.getName(n->getEdgeTypeID()).value());
        }

        else if (const auto* n = dynamic_cast<FilterNodeLabelNode*>(node.get())) {
            const auto* labelset = n->getLabelSet();
            std::vector<LabelID> labels;
            labelset->decompose(labels);
            for (const auto& label : labels) {
                output << fmt::format("        label {}\n", labelMap.getName(label).value());
            }
        }

        output << "    }\n";

        // Writing connections
        for (const PlanGraphNode* out : node->outputs()) {
            output << fmt::format("    {} ||--o{{ {} : _\n", fmt::ptr(node.get()), fmt::ptr(out));
        }
    }
}
