#include "CallPropertyStep.h"

using namespace db;

CallPropertyStep::CallPropertyStep(ColumnVector<PropertyTypeID>* id,
                                   ColumnVector<std::string>* propName,
                                   ColumnVector<std::string>* propType)
    : _id(id),
    _propName(propName),
    _propType(propType)
{
}

CallPropertyStep::~CallPropertyStep() {
}

void CallPropertyStep::execute() {
    _id->clear();
    _propName->clear();
    _propType->clear();

    const GraphReader& reader = _view->read();
    reader.getGraphProperties(_id, _propName, _propType);
}

void CallPropertyStep::describe(std::string& descr) const {
    descr.assign("CallPropertyStep");
}
