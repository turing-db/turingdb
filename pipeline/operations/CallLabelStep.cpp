#include "CallLabelStep.h"

using namespace db;

CallLabelStep::CallLabelStep(ColumnVector<LabelID>* id,
                                   ColumnVector<std::string>* labelName)
    : _id(id),
    _labelName(labelName)
{
}

CallLabelStep::~CallLabelStep() {
}

void CallLabelStep::execute() {
    _id->clear();
    _labelName->clear();

    const GraphReader& reader = _view->read();
    reader.getGraphLabels(_id, _labelName);
}

void CallLabelStep::describe(std::string& descr) const {
    descr.assign("CallLabelStep");
}
