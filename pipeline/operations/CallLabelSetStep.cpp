#include "CallLabelSetStep.h"

#include "GraphMetadata.h"
#include "labels/LabelSetMap.h"

using namespace db;

CallLabelSetStep::CallLabelSetStep(ColumnVector<LabelSetID>* id,
                                   ColumnVector<std::string_view>* labelNames)
    : _id(id),
    _labelNames(labelNames)
{
}

CallLabelSetStep::~CallLabelSetStep() {
}

void CallLabelSetStep::execute() {
    _id->clear();
    _labelNames->clear();

    const LabelSetMap& labelSetMap = _view->metadata().labelsets();
    const std::unordered_map<LabelSetID, std::unique_ptr<LabelSet>>& idMap = labelSetMap._forwardMap;

    const LabelMap& labelMap = _view->metadata().labels();
    const std::unordered_map<LabelID, std::string_view>& labelIdMap = labelMap._idMap;

    for (const auto& entry : idMap) {
        // Loop through all the existing labelIds to 'decompose' our labelset
        for (const auto& labelEntry : labelIdMap) {
            if (entry.second->hasLabel(labelEntry.first)) {
                _id->emplace_back(entry.first);
                _labelNames->emplace_back(labelEntry.second);
            }
        }
    }
}

void CallLabelSetStep::describe(std::string& descr) const {
    descr.assign("CallLabelSetStep");
}
