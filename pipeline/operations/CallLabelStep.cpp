#include "CallLabelStep.h"

#include "GraphMetadata.h"
#include "labels/LabelMap.h"

using namespace db;

CallLabelStep::CallLabelStep(ColumnVector<LabelID>* id,
                                   ColumnVector<std::string_view>* labelName)
    : _id(id),
    _labelName(labelName)
{
}

CallLabelStep::~CallLabelStep() {
}

void CallLabelStep::execute() {
    _id->clear();
    _labelName->clear();

    const LabelMap& labelMap = _view->metadata().labels();
    const std::unordered_map<LabelID, std::string_view>& idMap = labelMap._idMap;

    for (const auto& entry : idMap) {
        _id->emplace_back(entry.first);
        _labelName->emplace_back(entry.second);
    };
}

void CallLabelStep::describe(std::string& descr) const {
    descr.assign("CallLabelStep");
}
