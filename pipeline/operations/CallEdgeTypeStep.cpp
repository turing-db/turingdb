#include "CallEdgeTypeStep.h"

#include "GraphMetadata.h"
#include "types/EdgeTypeMap.h"

using namespace db;

CallEdgeTypeStep::CallEdgeTypeStep(ColumnVector<EdgeTypeID>* id,
                                   ColumnVector<std::string_view>* edgeTypeName)
    : _id(id),
    _edgeTypeName(edgeTypeName)
{
}

CallEdgeTypeStep::~CallEdgeTypeStep() {
}

void CallEdgeTypeStep::execute() {
    _id->clear();
    _edgeTypeName->clear();

    const EdgeTypeMap& edgeTypeMap = _view->metadata().edgeTypes();
    const std::unordered_map<EdgeTypeID, std::string_view>& idMap = edgeTypeMap._idMap;

    for (const auto& entry : idMap) {
        _id->emplace_back(entry.first);
        _edgeTypeName->emplace_back(entry.second);
    };
}

void CallEdgeTypeStep::describe(std::string& descr) const {
    descr.assign("CallEdgeTypeStep");
}
