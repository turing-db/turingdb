#include "PlanGraph.h"

using namespace db;

PlanGraph::PlanGraph()
{
}

PlanGraph::~PlanGraph() {
}

void PlanGraph::dump(std::ostream& out) const {
    for (const auto& node : _nodes) {
        out << "node id=" << std::hex << node.get()
            << " opcode=" << PlanGraphOpcodeDescription::value(node->getOpcode())
            << " [\n";
        out << "    inputs = [ ";

        for (const PlanGraphNode* input : node->inputs()) {
           out << std::hex << input << " "; 
        }

        out << "]\n";
        out << "    outputs = [ ";

        for (const PlanGraphNode* output : node->outputs()) {
           out << std::hex << output << " "; 
        }

        out << "]\n";

        out << "]\n\n";
    }
}
