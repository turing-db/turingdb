#include "EdgesProcedure.h"

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
#include "views/EdgeView.h"

using namespace db;

namespace {

struct Data : public ProcedureData {
    size_t _i {0};
};

}

ProcedureData* EdgesProcedure::allocData() {
    return new Data();
}

void EdgesProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void EdgesProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("edges");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addArgument("edgeID", ProcedureType::EDGE);
    proc->addReturnValue("edgeID", ProcedureType::EDGE);
    ns->addProcedure(proc);
}

void EdgesProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const Column* rawInputCol = data.getInputColumn(0);
    auto* outIdsCol = static_cast<ColumnEdgeIDs*>(data.getReturnColumn(0));

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE: {
            bioassert(rawInputCol, "db.edges: must be provided an edgeID column");

            const auto containerKind = ColumnKind::extractContainerKind(rawInputCol->getKind());
            bioassert(containerKind == ContainerKind::code<ColumnVector<void>>(),
                      "db.edges: edgeID input must be a vector column");
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

            const auto* inputIDs = static_cast<const ColumnEdgeIDs*>(rawInputCol);
            const GraphReader reader = ctxt->getGraphView()->read();

            const size_t total = inputIDs->size();
            const size_t remaining = std::min(total - data._i, ctxt->getChunkSize());
            const size_t end = data._i + remaining;

            for (size_t row = data._i; row < end; row++) {
                const EdgeID edgeID = (*inputIDs)[row];
                const EdgeView edge = reader.getEdgeView(edgeID);
                if (!edge.isValid()) {
                    throw ProcedureException(fmt::format("db.edges: invalid edge ID: {}",
                                                         edgeID.getValue()));
                }

                outIdsCol->push_back(edgeID);
            }

            data._i = end;

            if (data._i == total) {
                proc->finish();
            }
        }
        break;
    }
}
