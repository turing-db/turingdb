#include "ScanEdgesStep.h"

#include <sstream>

using namespace db;

ScanEdgesStep::ScanEdgesStep(const EdgeWriteInfo& edgeWriteInfo)
    : _edgeWriteInfo(edgeWriteInfo)
{
}

ScanEdgesStep::~ScanEdgesStep() {
}

void ScanEdgesStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ScanEdgesStep";
    ss << " indices=" << std::hex << _edgeWriteInfo._indices;
    ss << " sourceNodes=" << std::hex << _edgeWriteInfo._sourceNodes;
    ss << " edges=" << std::hex << _edgeWriteInfo._edges;
    ss << " targetNodes=" << std::hex << _edgeWriteInfo._targetNodes;
    ss << " edgeTypes=" << std::hex << _edgeWriteInfo._edgeTypes;
    descr.assign(ss.str());
}
