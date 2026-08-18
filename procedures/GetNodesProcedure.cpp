#include "GetNodesProcedure.h"

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
#include "views/NodeView.h"
#include "views/NodeEdgeView.h"
#include "views/EntityPropertyView.h"
#include "reader/GraphReader.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelMap.h"
#include "metadata/PropertyTypeMap.h"
#include "metadata/PropertyType.h"
#include "metadata/LabelSetHandle.h"
#include "versioning/Tombstones.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "ID.h"

using namespace db;

namespace {

using NodeIDCol = ColumnVector<NodeID>;
using ListColumn = ColumnVector<ListView>;
using UInt64Col = ColumnVector<types::UInt64::Primitive>;
using StringColumn = ColumnVector<std::string>;

constexpr std::string_view nodeIDsErr = "getNodes: nodeIDs must be a constant list";

struct Data : public ProcedureData {};

void executeImpl(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const Column* inputNodeIDs = data.getInputColumn(0);

    auto* idCol = static_cast<NodeIDCol*>(data.getReturnColumn(0));
    auto* labelsCol = static_cast<ListColumn*>(data.getReturnColumn(1));
    auto* inCol = static_cast<UInt64Col*>(data.getReturnColumn(2));
    auto* outCol = static_cast<UInt64Col*>(data.getReturnColumn(3));
    auto* propsCol = static_cast<StringColumn*>(data.getReturnColumn(4));

    const GraphView& view = *ctxt->getGraphView();
    const GraphReader reader(view);
    const GraphMetadata& metadata = reader.getMetadata();
    const LabelMap& labelMap = metadata.labels();
    const PropertyTypeMap& propTypes = metadata.propTypes();
    ListBuffer<4096>* listBuffer = ctxt->getListBuffer();

    const bool hasTombstones = view.tombstones().hasNodes();

    std::vector<int64_t> nodeIDs;
    const auto& nodeIDList = ProcUtils::constArg<ListView>(inputNodeIDs, nodeIDsErr);
    ProcUtils::readIntList(&nodeIDList, nodeIDs);

    if (idCol) {
        idCol->clear();
    }
    if (labelsCol) {
        labelsCol->clear();
    }
    if (inCol) {
        inCol->clear();
    }
    if (outCol) {
        outCol->clear();
    }
    if (propsCol) {
        propsCol->clear();
    }

    std::vector<LabelID> labelIDs;
    std::vector<ListBuffer<4096>::ListItemVariant> items;
    std::string propsJson;

    for (const int64_t rawID : nodeIDs) {
        const NodeID nodeID {static_cast<NodeID::Type>(rawID)};
        if (hasTombstones && reader.nodeIsDeleted(nodeID)) {
            continue; // deleted node
        }
        const NodeView node = reader.getNodeView(nodeID);
        if (!node.isValid()) {
            throw ProcedureException("Invalid node ID: " + std::to_string(rawID));
        }

        if (idCol) {
            idCol->push_back(node.nodeID());
        }

        if (labelsCol) {
            labelIDs.clear();
            node.labelset().decompose(labelIDs);
            items.clear();
            items.reserve(labelIDs.size());
            for (const LabelID id : labelIDs) {
                const auto name = labelMap.getName(id);
                if (name) {
                    items.emplace_back(types::String::Primitive {name.value()});
                }
            }
            labelsCol->push_back(listBuffer->insert(items));
        }

        if (inCol) {
            inCol->push_back(node.edges().getInEdgeCount());
        }

        if (outCol) {
            outCol->push_back(node.edges().getOutEdgeCount());
        }

        if (propsCol) {
            ProcUtils::encodeProperties(node.properties(), propTypes, propsJson);
            propsCol->push_back(propsJson);
        }
    }

    proc->finish();
}

}

ProcedureData* GetNodesProcedure::allocData() {
    return new Data();
}

void GetNodesProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void GetNodesProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("getNodes");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addConstantArgument("nodeIDs", ProcedureType::LIST);
    proc->addReturnValue("id", ProcedureType::NODE);
    proc->addReturnValue("labels", ProcedureType::LIST);
    proc->addReturnValue("inEdgeCount", ProcedureType::UINT_64);
    proc->addReturnValue("outEdgeCount", ProcedureType::UINT_64);
    proc->addReturnValue("properties", ProcedureType::STRING);
    ns->addProcedure(proc);
}

void GetNodesProcedure::execute(ProcedureState* proc) {
    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        executeImpl(proc);
        break;
    }
}
