#include "GetOutEdgesStep.h"

#include <sstream>

using namespace db;

GetOutEdgesStep::GetOutEdgesStep(const ColumnNodeIDs* inputNodeIDs,
                                 const EdgeWriteInfo& edgeWriteInfo)
    : _inputNodeIDs(inputNodeIDs),
    _edgeWriteInfo(edgeWriteInfo)
{
}

GetOutEdgesStep::~GetOutEdgesStep() {
}

void GetOutEdgesStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "GetOutEdgesStep";
    ss << " inputNodeIDs=" << std::hex << _inputNodeIDs;
    ss << " indices=" << std::hex << _edgeWriteInfo._indices;
    ss << " sourceNodes=" << std::hex << _edgeWriteInfo._sourceNodes;
    ss << " edges=" << std::hex << _edgeWriteInfo._edges;
    ss << " targetNodes=" << std::hex << _edgeWriteInfo._targetNodes;
    ss << " edgeTypes=" << std::hex << _edgeWriteInfo._edgeTypes;
    descr.assign(ss.str());
}
