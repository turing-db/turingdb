#include "HierarchicalLabelCountsProcedure.h"

#include "ProcedureContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "ProcUtils.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelMap.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "list/ListView.h"
#include "list/ListElementView.h"
#include "list/ListBufferTypeTag.h"

using namespace db;

namespace {

using StringViewCol = ColumnVector<std::string_view>;
using UInt64Col = ColumnVector<types::UInt64::Primitive>;

constexpr std::string_view currentLabelsErr = "hierarchicalLabelCounts: currentLabels must be a constant list";

struct Data : public ProcedureData {};

// Read the string elements of `view` into `selected` as the set of selected
// labels. The caller supplies the list view via `ProcUtils::constArg<ListView>`,
// which enforces the constant-list rule.
void readSelectedLabels(const ListView* view, const LabelMap& labels, LabelSet& selected) {
    for (const ListElementView& el : *view) {
        if (el.getTag() != ListBufferTypeTag::String) {
            continue;
        }
        const std::string_view name = el.getAs<std::string_view>();
        const auto id = labels.get(name);
        if (id) {
            selected.set(id.value());
        }
    }
}

void executeImpl(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    auto* namesCol = static_cast<StringViewCol*>(data.getReturnColumn(0));
    auto* countCol = static_cast<UInt64Col*>(data.getReturnColumn(1));

    const GraphView& view = *ctxt->getGraphView();
    const GraphReader reader(view);
    const LabelMap& labels = reader.getMetadata().labels();

    LabelSet selected;
    const auto& currentLabels = ProcUtils::constArg<ListView>(data.getInputColumn(0), currentLabelsErr);
    readSelectedLabels(&currentLabels, labels, selected);

    if (namesCol) {
        namesCol->clear();
    }
    if (countCol) {
        countCol->clear();
    }

    // For every label not already selected, count the nodes matching
    // (selected ∪ {label}).
    const size_t labelCount = labels.getCount();
    LabelSet combined;
    for (size_t i = 0; i < labelCount; ++i) {
        const LabelID id = (LabelID)i;
        if (selected.hasLabel(id)) {
            continue;
        }

        combined = selected;
        combined.set(id);

        const size_t nodeCount = reader.getNodeCountMatchingLabelset(LabelSetHandle {combined});

        if (nodeCount == 0) {
            continue;
        }

        if (namesCol) {
            const auto name = labels.getName(id);
            namesCol->push_back(name ? name.value() : std::string_view {});
        }
        if (countCol) {
            countCol->push_back(nodeCount);
        }
    }

    proc->finish();
}

}

ProcedureData* HierarchicalLabelCountsProcedure::allocData() {
    return new Data();
}

void HierarchicalLabelCountsProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void HierarchicalLabelCountsProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("hierarchicalLabelCounts");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addConstantArgument("currentLabels", ProcedureType::LIST);
    proc->addReturnValue("label", ProcedureType::STRING_VIEW);
    proc->addReturnValue("nodeCount", ProcedureType::UINT_64);
    ns->addProcedure(proc);
}

void HierarchicalLabelCountsProcedure::execute(ProcedureState* proc) {
    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        executeImpl(proc);
        break;
    }
}
