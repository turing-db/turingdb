#include "GetOutEdgesStep.h"

using namespace db;

GetOutEdgesStep::GetOutEdgesStep(const ColumnIDs* inputNodeIDs,
                                 const EdgeWriteInfo& edgeWriteInfo)
    : _inputNodeIDs(inputNodeIDs),
    _edgeWriteInfo(edgeWriteInfo)
{
}

GetOutEdgesStep::~GetOutEdgesStep() {
}
