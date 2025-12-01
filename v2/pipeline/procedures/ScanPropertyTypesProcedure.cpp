#include "ScanPropertyTypesProcedure.h"

#include "ExecutionContext.h"
#include "procedures/Procedure.h"
#include "iterators/ScanPropertyTypesIterator.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"

#include "PipelineException.h"

using namespace db::v2;

std::unique_ptr<ProcedureData> ScanPropertyTypesProcedure::allocData() {
    return std::make_unique<Data>();
}

void ScanPropertyTypesProcedure::execute(Procedure& proc) {
    Data& data = proc.data<Data>();
    const ExecutionContext* ctxt = proc.ctxt();
    const GraphView& _view = ctxt->getGraphView();

    Column* rawIdsCol = data.getReturnColumn(0);
    Column* rawNamesCol = data.getReturnColumn(1);
    Column* rawValueTypesCol = data.getReturnColumn(2);

    auto* idsCol = static_cast<ColumnVector<PropertyTypeID>*>(rawIdsCol);
    auto* namesCol = static_cast<ColumnVector<std::string_view>*>(rawNamesCol);
    auto* valueTypesCol = static_cast<ColumnVector<ValueType>*>(rawValueTypesCol);

    switch (proc.step()) {
        case Procedure::Step::PREPARE: {
            data._it = std::make_unique<ScanPropertyTypesChunkWriter>(_view.metadata().propTypes());
            data._it->setPropertyTypes(idsCol);
            data._it->setNames(namesCol);
            data._it->setValueTypes(valueTypesCol);
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

