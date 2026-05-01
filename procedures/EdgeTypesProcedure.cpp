#include "EdgeTypesProcedure.h"

#include "ProcedureContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "iterators/ScanEdgeTypesIterator.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"

using namespace db;

namespace {

struct Data : public ProcedureData {
    std::unique_ptr<ScanEdgeTypesChunkWriter> _it;
};

}

ProcedureData* EdgeTypesProcedure::allocData() {
    return new Data();
}

void EdgeTypesProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void EdgeTypesProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("edgeTypes");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addReturnValue("id", ProcedureType::EDGE_TYPE_ID);
    proc->addReturnValue("edgeType", ProcedureType::STRING_VIEW);
    ns->addProcedure(proc);
}

void EdgeTypesProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();
    const GraphView& view = *ctxt->getGraphView();

    Column* rawIdsCol = data.getReturnColumn(0);
    Column* rawNamesCol = data.getReturnColumn(1);

    auto* idsCol = static_cast<ColumnVector<EdgeTypeID>*>(rawIdsCol);
    auto* namesCol = static_cast<ColumnVector<std::string_view>*>(rawNamesCol);

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE: {
            data._it = std::make_unique<ScanEdgeTypesChunkWriter>(view.metadata().edgeTypes());

            if (idsCol) {
                data._it->setIDs(idsCol);
            }

            if (namesCol) {
                data._it->setNames(namesCol);
            }
        }
        break;

        case ProcedureState::Step::RESET: {
            data._it->reset();
        }
        break;

        case ProcedureState::Step::EXECUTE: {
            if (!idsCol && !namesCol) {
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
