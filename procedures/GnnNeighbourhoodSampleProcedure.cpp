#include "GnnNeighbourhoodSampleProcedure.h"

#include <cstdint>

#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureException.h"
#include "ProcedureNamespace.h"
#include "ProcedureState.h"
#include "ProcUtils.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/NeighbourhoodSampleIterator.h"
#include "views/GraphView.h"
#include "ID.h"

using namespace db;

namespace {

constexpr std::string_view sampleSizeErr = "gnn.neighbourhood_sample: sampleSize must be a constant int";

struct Data : public ProcedureData {};

void executeImpl(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const auto* inputNodeIDs = static_cast<const ColumnNodeIDs*>(data.getInputColumn(0));
    const Column* inputSampleSize = data.getInputColumn(1);

    auto* srcCol = static_cast<ColumnNodeIDs*>(data.getReturnColumn(0));
    auto* edgeCol = static_cast<ColumnEdgeIDs*>(data.getReturnColumn(1));
    auto* edgeTypeCol = static_cast<ColumnEdgeTypes*>(data.getReturnColumn(2));
    auto* dstCol = static_cast<ColumnNodeIDs*>(data.getReturnColumn(3));

    const GraphView& view = *ctxt->getGraphView();

    const int64_t signedSampleSize =
        ProcUtils::constArg<types::Int64::Primitive>(inputSampleSize, sampleSizeErr);
    if (signedSampleSize <= 0) {
        throw ProcedureException("gnn.neighbourhood_sample: sampleSize must be positive");
    }
    const size_t sampleSize = static_cast<size_t>(signedSampleSize);

    if (!inputNodeIDs || inputNodeIDs->size() == 0) {
        proc->finish();
        return;
    }

    NeighbourhoodSampleChunkWriter writer(view, inputNodeIDs, sampleSize);
    writer.setOutputColumns(srcCol, edgeCol, edgeTypeCol, dstCol);
    writer.fill(ChunkConfig::CHUNK_SIZE);

    proc->finish();
}

}

ProcedureData* GnnNeighbourhoodSampleProcedure::allocData() {
    return new Data();
}

void GnnNeighbourhoodSampleProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void GnnNeighbourhoodSampleProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("neighbourhoodSample");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addArgument("node", ProcedureType::NODE);
    proc->addArgument("sampleSize", ProcedureType::INT64);
    proc->addReturnValue("src", ProcedureType::NODE);
    proc->addReturnValue("edge", ProcedureType::EDGE);
    proc->addReturnValue("edgeType", ProcedureType::EDGE_TYPE_ID);
    proc->addReturnValue("dst", ProcedureType::NODE);
    ns->addProcedure(proc);
}

void GnnNeighbourhoodSampleProcedure::execute(ProcedureState* proc) {
    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        executeImpl(proc);
        break;
    }
}
