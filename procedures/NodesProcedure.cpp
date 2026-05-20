#include "NodesProcedure.h"

#include <algorithm>

#include <spdlog/fmt/fmt.h>

#include "ProcedureContext.h"
#include "ProcedureException.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "reader/GraphReader.h"
#include "views/GraphView.h"
#include "views/NodeView.h"

using namespace db;

namespace {

struct Data : public ProcedureData {
    size_t _i {0};
};

}

ProcedureData* NodesProcedure::allocData() {
    return new Data();
}

void NodesProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void NodesProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("nodes");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addArgument("nodeID", ProcedureType::NODE);
    proc->addReturnValue("nodeID", ProcedureType::NODE);
    ns->addProcedure(proc);
}

void NodesProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const Column* rawInputCol = data.getInputColumn(0);
    auto* outIdsCol = static_cast<ColumnNodeIDs*>(data.getReturnColumn(0));

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE: {
            bioassert(rawInputCol, "db.nodes: must be provided a nodeID column");

            const auto containerKind = ColumnKind::extractContainerKind(rawInputCol->getKind());
            bioassert(containerKind == ContainerKind::code<ColumnVector<void>>(),
                      "db.nodes: nodeID input must be a vector column");
        }
        break;

        case ProcedureState::Step::RESET: {
            data._i = 0;
        }
        break;

        case ProcedureState::Step::EXECUTE: {
            if (!outIdsCol) {
                proc->finish();
                break;
            }

            outIdsCol->clear();

            const auto* inputIDs = static_cast<const ColumnNodeIDs*>(rawInputCol);
            const GraphReader reader = ctxt->getGraphView()->read();

            const size_t total = inputIDs->size();
            const size_t remaining = std::min(total - data._i, ctxt->getChunkSize());
            const size_t end = data._i + remaining;

            for (size_t row = data._i; row < end; row++) {
                const NodeID nodeID = (*inputIDs)[row];
                const NodeView node = reader.getNodeView(nodeID);
                if (!node.isValid()) {
                    throw ProcedureException(fmt::format("db.nodes: invalid node ID: {}",
                                                         nodeID.getValue()));
                }

                outIdsCol->push_back(nodeID);
            }

            data._i = end;

            if (data._i == total) {
                proc->finish();
            }
        }
        break;
    }
}
