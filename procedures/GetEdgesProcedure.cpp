#include "GetEdgesProcedure.h"

#include <cstdint>
#include <string>
#include <vector>

#include "ProcedureContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "ProcUtils.h"
#include "ProcedureException.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"
#include "views/EdgeView.h"
#include "views/EntityPropertyView.h"
#include "reader/GraphReader.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyTypeMap.h"
#include "versioning/Tombstones.h"
#include "list/ListView.h"
#include "ID.h"

using namespace db;

namespace {

using EdgeIDCol = ColumnVector<EdgeID>;
using NodeIDCol = ColumnVector<NodeID>;
using EdgeTypeIDCol = ColumnVector<EdgeTypeID>;
using StringColumn = ColumnVector<std::string>;

constexpr std::string_view edgeIDsErr = "getEdges: edgeIDs must be a constant list";

struct Data : public ProcedureData {};

void executeImpl(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const Column* inputEdgeIDs = data.getInputColumn(0);

    auto* idCol = static_cast<EdgeIDCol*>(data.getReturnColumn(0));
    auto* srcCol = static_cast<NodeIDCol*>(data.getReturnColumn(1));
    auto* tgtCol = static_cast<NodeIDCol*>(data.getReturnColumn(2));
    auto* typeCol = static_cast<EdgeTypeIDCol*>(data.getReturnColumn(3));
    auto* propsCol = static_cast<StringColumn*>(data.getReturnColumn(4));

    const GraphView& view = *ctxt->getGraphView();
    const GraphReader reader(view);
    const PropertyTypeMap& propTypes = reader.getMetadata().propTypes();

    const bool hasEdgeTombstones = view.tombstones().hasEdges();

    std::vector<int64_t> edgeIDs;
    const auto& edgeIDsList = ProcUtils::constArg<ListView>(inputEdgeIDs, edgeIDsErr);
    ProcUtils::readIntList(&edgeIDsList, edgeIDs);

    if (idCol) {
        idCol->clear();
    }

    if (srcCol) {
        srcCol->clear();
    }

    if (tgtCol) {
        tgtCol->clear();
    }

    if (typeCol) {
        typeCol->clear();
    }

    if (propsCol) {
        propsCol->clear();
    }

    std::string propsJson;

    for (const int64_t rawID : edgeIDs) {
        const EdgeID edgeID {static_cast<EdgeID::Type>(rawID)};
        if (hasEdgeTombstones && reader.edgeIsDeleted(edgeID)) {
            continue; // deleted edge
        }
        const EdgeView edge = reader.getEdgeView(edgeID);
        if (!edge.isValid()) {
            throw ProcedureException("Invalid edge ID: " + std::to_string(rawID));
        }
        if (idCol) {
            idCol->push_back(edge.id());
        }

        if (srcCol) {
            srcCol->push_back(edge.sourceID());
        }

        if (tgtCol) {
            tgtCol->push_back(edge.targetID());
        }

        if (typeCol) {
            typeCol->push_back(edge.edgeTypeID());
        }

        if (propsCol) {
            ProcUtils::encodeProperties(edge.properties(), propTypes, propsJson);
            propsCol->push_back(propsJson);
        }
    }

    proc->finish();
}

}

ProcedureData* GetEdgesProcedure::allocData() {
    return new Data();
}

void GetEdgesProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void GetEdgesProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("getEdges");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addConstantArgument("edgeIDs", ProcedureType::LIST);
    proc->addReturnValue("id", ProcedureType::EDGE);
    proc->addReturnValue("src", ProcedureType::NODE);
    proc->addReturnValue("tgt", ProcedureType::NODE);
    proc->addReturnValue("edgeTypeID", ProcedureType::EDGE_TYPE_ID);
    proc->addReturnValue("properties", ProcedureType::STRING);
    ns->addProcedure(proc);
}

void GetEdgesProcedure::execute(ProcedureState* proc) {
    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        executeImpl(proc);
        break;
    }
}
