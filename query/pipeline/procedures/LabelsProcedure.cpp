#include "LabelsProcedure.h"

#include "ExecutionContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "iterators/ScanLabelsIterator.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"

#include "PipelineException.h"

using namespace db;

ProcedureData* LabelsProcedure::allocData() {
    return new Data();
}

void LabelsProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void LabelsProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("labels");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addReturnValue("id", ProcedureType::LABEL_ID);
    proc->addReturnValue("label", ProcedureType::STRING_VIEW);
    ns->addProcedure(proc);
}

void LabelsProcedure::execute(ProcedureState& proc) {
    Data& data = proc.data<Data>();
    const ExecutionContext* ctxt = proc.ctxt();
    const GraphView& view = ctxt->getGraphView();

    Column* rawIdsCol = data.getReturnColumn(0);
    Column* rawNamesCol = data.getReturnColumn(1);

    auto* idsCol = static_cast<ColumnVector<LabelID>*>(rawIdsCol);
    auto* namesCol = static_cast<ColumnVector<std::string_view>*>(rawNamesCol);

    switch (proc.step()) {
        case ProcedureState::Step::PREPARE: {
            data._it = std::make_unique<ScanLabelsChunkWriter>(
                view.metadata().labels());
            data._it->setIDs(idsCol);
            data._it->setNames(namesCol);
            return;
        }

        case ProcedureState::Step::RESET: {
            data._it->reset();
            return;
        }

        case ProcedureState::Step::EXECUTE: {
            data._it->fill(proc.ctxt()->getChunkSize());

            if (!data._it->isValid()) {
                proc.finish();
            }

            return;
        }
    }

    throw PipelineException("Unknown procedure step");
}
