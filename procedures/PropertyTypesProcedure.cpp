#include "PropertyTypesProcedure.h"

#include "ProcedureContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "TypedProcedure.h"
#include "iterators/ScanPropertyTypesIterator.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"

using namespace db;

namespace {

struct Data : public ProcedureData {
    std::unique_ptr<ScanPropertyTypesChunkWriter> _it;
};

}

void PropertyTypesProcedure::registerProcedure(ProcedureNamespace* ns) {
    ns->addProcedure(createTypedProcedure<PropertyTypesProcedure, Data>("propertyTypes"));
}

void PropertyTypesProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();
    const GraphView& view = *ctxt->getGraphView();

    auto* idsCol = getReturnColumn<PropertyTypesProcedure, 0>(&data);
    auto* namesCol = getReturnColumn<PropertyTypesProcedure, 1>(&data);
    auto* valueTypesCol = getReturnColumn<PropertyTypesProcedure, 2>(&data);

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE: {
            data._it = std::make_unique<ScanPropertyTypesChunkWriter>(view.metadata().propTypes());

            if (idsCol) {
                data._it->setPropertyTypes(idsCol);
            }

            if (namesCol) {
                data._it->setNames(namesCol);
            }

            if (valueTypesCol) {
                data._it->setValueTypes(valueTypesCol);
            }
        }
        break;

        case ProcedureState::Step::RESET: {
            data._it->reset();
        }
        break;

        case ProcedureState::Step::EXECUTE: {
            if (!idsCol && !namesCol && !valueTypesCol) {
                proc->finish();
                break;
            }

            data._it->fill(proc->getContext()->getChunkSize());

            if (!data._it->isValid()) {
                proc->finish();
            }
        }
        break;
    }
}
