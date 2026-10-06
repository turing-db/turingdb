#include "GetNodesProcedure.h"

#include <cstdint>
#include <string>
#include <vector>

#include "ProcedureContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "TypedProcedure.h"
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
#include "buffers/StringBuffer.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "ID.h"

using namespace db;

namespace {

constexpr std::string_view nodeIDsErr = "getNodes: nodeIDs must be a constant list";

struct Data : public TypedProcedureData<GetNodesProcedure> {};

void executeImpl(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const Column* inputNodeIDs = data.getInputColumn(0);

    auto* idCol = data.getReturnColumn<0>();
    auto* labelsCol = data.getReturnColumn<1>();
    auto* inCol = data.getReturnColumn<2>();
    auto* outCol = data.getReturnColumn<3>();
    auto* propsCol = data.getReturnColumn<4>();

    const GraphView& view = *ctxt->getGraphView();
    const GraphReader reader(view);
    const GraphMetadata& metadata = reader.getMetadata();
    const LabelMap& labelMap = metadata.labels();
    const PropertyTypeMap& propTypes = metadata.propTypes();
    ListBuffer<4096>* listBuffer = ctxt->getListBuffer();
    StringBuffer* stringBuffer = ctxt->getStringBuffer();

    const bool hasTombstones = view.hasDeletedNodes();

    std::vector<int64_t> nodeIDs;
    const auto& nodeIDList = ProcUtils::constArg<ListView>(inputNodeIDs, nodeIDsErr);
    ProcUtils::readIntList(&nodeIDList, nodeIDs);

    data.clearReturnColumns();

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
            propsCol->push_back(stringBuffer->insert(propsJson));
        }
    }

    proc->finish();
}

}

void GetNodesProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = createTypedProcedure<Data>("getNodes");
    proc->addConstantArgument("nodeIDs", ProcedureType::LIST);
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
