#include "GMLDumper.h"

#include "Network.h"
#include "Node.h"
#include "Edge.h"

using namespace db;

GMLDumper::GMLDumper(const db::Network* net, const Path& gmlFilePath)
    : _net(net),
    _gmlFilePath(gmlFilePath)
{
}

GMLDumper::~GMLDumper() {
}

bool GMLDumper::dump() {
    _gml.open(_gmlFilePath, std::ofstream::trunc | std::ofstream::out);
    if (!_gml.is_open()) {
        return false;
    }

    _gml << "graph [\n";
    for (const auto& [id, node] : _net->nodes()) {
        _gml << "    node [\n";
        _gml << "        id "
             << std::to_string(node->getIndex().getObjectID()) << "\n";
        _gml << "    ]\n";
    }

    for (const auto& [id, edge] : _net->edges()) {
        _gml << "    edge [\n";
        _gml << "        source "
             << std::to_string(edge->getSource()->getIndex().getObjectID()) << "\n";
        _gml << "        target "
             << std::to_string(edge->getTarget()->getIndex().getObjectID()) << "\n";
        _gml << "    ]\n";
    }
    _gml << "]\n";

    return true;
}
