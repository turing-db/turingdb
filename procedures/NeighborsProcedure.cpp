#include "NeighborsProcedure.h"

#include "ProcedureContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetInEdgesIterator.h"
#include "spdlog/spdlog.h"
#include "views/GraphView.h"
#include "metadata/PropertyType.h"

using namespace db;

namespace {

enum class Phase { Out, In };

struct Data : public ProcedureData {
    Phase _phase {Phase::Out};
    std::unique_ptr<GetOutEdgesChunkWriter> _outIt;
    std::unique_ptr<GetInEdgesChunkWriter> _inIt;
    ColumnVector<size_t> _indices;
};

}

ProcedureData* NeighborsProcedure::allocData() {
    return new Data();
}

void NeighborsProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void NeighborsProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("neighbors");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addArgument("nodeID", ProcedureType::NODE);
    proc->addReturnValue("sourceNodeID", ProcedureType::NODE);
    proc->addReturnValue("edgeID", ProcedureType::EDGE);
    proc->addReturnValue("otherNodeID", ProcedureType::NODE);
    proc->addReturnValue("isOutgoing", ProcedureType::BOOL);
    ns->addProcedure(proc);
}

void NeighborsProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const Column* rawInputCol = data.getInputColumn(0);
    auto* sourceNodeIDCol = static_cast<ColumnNodeIDs*>(data.getReturnColumn(0));
    auto* edgeIDCol = static_cast<ColumnEdgeIDs*>(data.getReturnColumn(1));
    auto* otherNodeIDCol = static_cast<ColumnNodeIDs*>(data.getReturnColumn(2));
    auto* isOutgoingCol = static_cast<ColumnVector<types::Bool::Primitive>*>(data.getReturnColumn(3));

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE: {
            bioassert(rawInputCol, "db.neighbors: must be provided a nodeID column");

            const auto containerKind = ColumnKind::extractContainerKind(rawInputCol->getKind());
            bioassert(containerKind == ContainerKind::code<ColumnVector<void>>(),
                      "db.neighbors: nodeID input must be a vector column");

            const auto* inputNodeIDs = static_cast<const ColumnNodeIDs*>(rawInputCol);
            const GraphView& view = *ctxt->getGraphView();

            data._outIt = std::make_unique<GetOutEdgesChunkWriter>(view, inputNodeIDs);
            data._outIt->setIndices(&data._indices);
            data._outIt->setEdgeIDs(edgeIDCol);
            data._outIt->setTgtIDs(otherNodeIDCol);

            data._inIt = std::make_unique<GetInEdgesChunkWriter>(view, inputNodeIDs);
            data._inIt->setIndices(&data._indices);
            data._inIt->setEdgeIDs(edgeIDCol);
            data._inIt->setSrcIDs(otherNodeIDCol);
        }
        break;

        case ProcedureState::Step::RESET: {
            data._phase = Phase::Out;
            data._outIt->reset();
            data._inIt->reset();
        }
        break;

        case ProcedureState::Step::EXECUTE: {
            const auto* inputNodeIDs = static_cast<const ColumnNodeIDs*>(rawInputCol);
            const size_t chunkSize = ctxt->getChunkSize();

            const auto fillAuxColumns = [&](bool isOutgoing) {
                const size_t produced = data._indices.size();

                if (sourceNodeIDCol) {
                    sourceNodeIDCol->clear();
                    sourceNodeIDCol->reserve(produced);
                    for (size_t i = 0; i < produced; i++) {
                        spdlog::info("indices is {}", data._indices[i]);
                        sourceNodeIDCol->push_back((*inputNodeIDs)[data._indices[i]]);
                    }
                }

                if (isOutgoingCol) {
                    isOutgoingCol->clear();
                    isOutgoingCol->reserve(produced);
                    for (size_t i = 0; i < produced; i++) {
                        isOutgoingCol->push_back(isOutgoing);
                    }
                }
            };

            if (data._phase == Phase::Out) {
                data._outIt->fill(chunkSize);
                fillAuxColumns(true);

                if (!data._outIt->isValid()) {
                    data._phase = Phase::In;
                }
            } else {
                data._inIt->fill(chunkSize);
                fillAuxColumns(false);

                if (!data._inIt->isValid()) {
                    proc->finish();
                }
            }
        }
        break;
    }
}
