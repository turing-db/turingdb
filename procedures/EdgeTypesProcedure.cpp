#include "EdgeTypesProcedure.h"

#include "ProcedureContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "TypedProcedure.h"
#include "iterators/ScanEdgeTypesIterator.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"

using namespace db;

namespace {

struct Data : public TypedProcedureData<EdgeTypesProcedure> {
    std::unique_ptr<ScanEdgeTypesChunkWriter> _it;
};

}

void EdgeTypesProcedure::registerProcedure(ProcedureNamespace* ns) {
    ns->addProcedure(createTypedProcedure<Data>("edgeTypes"));
}

void EdgeTypesProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();
    const GraphView& view = *ctxt->getGraphView();

    auto* idsCol = data.getReturnColumn<0>();
    auto* namesCol = data.getReturnColumn<1>();

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
