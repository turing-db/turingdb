#include "CallEdgeTypeStep.h"

using namespace db;

CallEdgeTypeStep::CallEdgeTypeStep(ColumnVector<EdgeTypeID>* id,
                                   ColumnVector<std::string>* edgeTypeName)
    : _id(id),
    _edgeTypeName(edgeTypeName)
{
}

CallEdgeTypeStep::~CallEdgeTypeStep() {
}

void CallEdgeTypeStep::execute() {
    _id->clear();
    _edgeTypeName->clear();

    const GraphReader& reader = _view->read();
    reader.getGraphEdgeTypes(_id, _edgeTypeName);
}

void CallEdgeTypeStep::describe(std::string& descr) const {
    descr.assign("CallEdgeTypeStep");
}
