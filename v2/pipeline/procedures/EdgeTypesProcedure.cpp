#include "EdgeTypesProcedure.h"

#include "ExecutionContext.h"
#include "procedures/Procedure.h"
#include "iterators/ScanEdgeTypesIterator.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"

#include "PipelineException.h"

using namespace db::v2;

std::unique_ptr<ProcedureData> EdgeTypesProcedure::allocData() {
    return std::make_unique<Data>();
}

void EdgeTypesProcedure::execute(Procedure& proc) {
    Data& data = proc.data<Data>();
    const ExecutionContext* ctxt = proc.ctxt();
    const GraphView& _view = ctxt->getGraphView();

    Column* rawIdsCol = data.getReturnColumn(0);
    Column* rawNamesCol = data.getReturnColumn(1);

    auto* idsCol = static_cast<ColumnVector<EdgeTypeID>*>(rawIdsCol);
    auto* namesCol = static_cast<ColumnVector<std::string_view>*>(rawNamesCol);

    switch (proc.step()) {
        case Procedure::Step::PREPARE: {
            data._it = std::make_unique<ScanEdgeTypesChunkWriter>(_view.metadata().edgeTypes());
            data._it->setIDs(idsCol);
            data._it->setNames(namesCol);
            return;
        }

        case Procedure::Step::RESET: {
            data._it->reset();
            return;
        }

        case Procedure::Step::EXECUTE: {
            data._it->fill(proc.ctxt()->getChunkSize());

            if (!data._it->isValid()) {
                proc.finish();
            }

            return;
        }
    }

    throw PipelineException("Unknown procedure step");
}

