#include "PropertyTypesProcedure.h"

#include "ExecutionContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "iterators/ScanPropertyTypesIterator.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"

#include "PipelineException.h"

using namespace db;

namespace {

struct Data : public ProcedureData {
    std::unique_ptr<ScanPropertyTypesChunkWriter> _it;
};

}

ProcedureData* PropertyTypesProcedure::allocData() {
    return new Data();
}

void PropertyTypesProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void PropertyTypesProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("propertyTypes");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addReturnValue("id", ProcedureType::PROPERTY_TYPE_ID);
    proc->addReturnValue("propertyType", ProcedureType::STRING_VIEW);
    proc->addReturnValue("valueType", ProcedureType::VALUE_TYPE);
    ns->addProcedure(proc);
}

void PropertyTypesProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ExecutionContext* ctxt = proc->getContext();
    const GraphView& view = ctxt->getGraphView();

    Column* rawIdsCol = data.getReturnColumn(0);
    Column* rawNamesCol = data.getReturnColumn(1);
    Column* rawValueTypesCol = data.getReturnColumn(2);

    auto* idsCol = static_cast<ColumnVector<PropertyTypeID>*>(rawIdsCol);
    auto* namesCol = static_cast<ColumnVector<std::string_view>*>(rawNamesCol);
    auto* valueTypesCol = static_cast<ColumnVector<ValueType>*>(rawValueTypesCol);

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
