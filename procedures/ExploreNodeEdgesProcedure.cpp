#include "ExploreNodeEdgesProcedure.h"

#include "ProcedureContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnVector.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetInEdgesIterator.h"
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

ProcedureData* ExploreNodeEdgesProcedure::allocData() {
    return new Data();
}

void ExploreNodeEdgesProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void ExploreNodeEdgesProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("exploreNodeEdges");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addArgument("nodeID", ProcedureType::NODE);
    proc->addReturnValue("sourceNodeID", ProcedureType::NODE);
    proc->addReturnValue("edgeID", ProcedureType::EDGE);
    proc->addReturnValue("edgeTypeID", ProcedureType::EDGE_TYPE_ID);
    proc->addReturnValue("otherNodeID", ProcedureType::NODE);
    proc->addReturnValue("isOutgoing", ProcedureType::BOOL);
    ns->addProcedure(proc);
}

void ExploreNodeEdgesProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const Column* rawInputCol = data.getInputColumn(0);
    auto* sourceNodeIDCol = static_cast<ColumnNodeIDs*>(data.getReturnColumn(0));
    auto* edgeIDCol = static_cast<ColumnEdgeIDs*>(data.getReturnColumn(1));
    auto* edgeTypeIDCol = static_cast<ColumnEdgeTypes*>(data.getReturnColumn(2));
    auto* otherNodeIDCol = static_cast<ColumnNodeIDs*>(data.getReturnColumn(3));
    auto* isOutgoingCol = static_cast<ColumnVector<types::Bool::Primitive>*>(data.getReturnColumn(4));

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE: {
            bioassert(rawInputCol, "db.exploreNodeEdges: must be provided a nodeID column");

            const auto containerKind = ColumnKind::extractContainerKind(rawInputCol->getKind());
            bioassert(containerKind == ContainerKind::code<ColumnVector<void>>(),
                      "db.exploreNodeEdges: nodeID input must be a vector column");

            const auto* inputNodeIDs = static_cast<const ColumnNodeIDs*>(rawInputCol);
            const GraphView& view = *ctxt->getGraphView();

            data._outIt = std::make_unique<GetOutEdgesChunkWriter>(view, inputNodeIDs);
            data._outIt->setIndices(&data._indices);
            data._outIt->setEdgeIDs(edgeIDCol);
            data._outIt->setTgtIDs(otherNodeIDCol);
            data._outIt->setEdgeTypes(edgeTypeIDCol);

            data._inIt = std::make_unique<GetInEdgesChunkWriter>(view, inputNodeIDs);
            data._inIt->setIndices(&data._indices);
            data._inIt->setEdgeIDs(edgeIDCol);
            data._inIt->setSrcIDs(otherNodeIDCol);
            data._inIt->setEdgeTypes(edgeTypeIDCol);
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
