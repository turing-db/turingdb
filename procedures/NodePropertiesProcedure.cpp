#include "NodePropertiesProcedure.h"

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
#include "views/EntityPropertyView.h"
#include "views/PropertyView.h"
#include "metadata/PropertyType.h"
#include "metadata/PropertyTypeMap.h"

using namespace db;

namespace {

struct Data : public ProcedureData {
    size_t _inputIdx {0};
};

std::string formatValue(const PropertyView& propView) {
    return std::visit([](const auto* p) -> std::string {
        using Ptr = decltype(p);
        using Pointed = std::remove_const_t<std::remove_pointer_t<Ptr>>;

        if constexpr (std::is_same_v<Pointed, types::String::Primitive>) {
            return std::string(*p);
        } else if constexpr (std::is_same_v<Pointed, types::Bool::Primitive>) {
            return static_cast<bool>(*p) ? "true" : "false";
        } else if constexpr (std::is_same_v<Pointed, types::Embedding::Primitive>) {
            std::string result = "[";
            const auto& span = *p;
            for (size_t i = 0; i < span.size(); i++) {
                if (i > 0) {
                    result += ",";
                }
                result += std::to_string(span[i]);
            }
            result += "]";
            return result;
        } else {
            return std::to_string(*p);
        }
    }, propView._value);
}

}

ProcedureData* NodePropertiesProcedure::allocData() {
    return new Data();
}

void NodePropertiesProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void NodePropertiesProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("nodeProperties");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addArgument("nodeID", ProcedureType::NODE);
    proc->addReturnValue("nodeID", ProcedureType::NODE);
    proc->addReturnValue("propertyName", ProcedureType::STRING_VIEW);
    proc->addReturnValue("value", ProcedureType::STRING);
    ns->addProcedure(proc);
}

void NodePropertiesProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const Column* rawInputCol = data.getInputColumn(0);
    auto* outNodeIDCol = static_cast<ColumnNodeIDs*>(data.getReturnColumn(0));
    auto* outNameCol = static_cast<ColumnVector<std::string_view>*>(data.getReturnColumn(1));
    auto* outValueCol = static_cast<ColumnVector<std::string>*>(data.getReturnColumn(2));

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE: {
            bioassert(rawInputCol, "db.nodeProperties: must be provided a nodeID column");

            const auto containerKind = ColumnKind::extractContainerKind(rawInputCol->getKind());
            bioassert(containerKind == ContainerKind::code<ColumnVector<void>>(),
                      "db.nodeProperties: nodeID input must be a vector column");
        }
        break;

        case ProcedureState::Step::RESET: {
            data._inputIdx = 0;
        }
        break;

        case ProcedureState::Step::EXECUTE: {
            if (outNodeIDCol) {
                outNodeIDCol->clear();
            }
            if (outNameCol) {
                outNameCol->clear();
            }
            if (outValueCol) {
                outValueCol->clear();
            }

            const auto* inputNodeIDs = static_cast<const ColumnNodeIDs*>(rawInputCol);
            const GraphReader reader = ctxt->getGraphView()->read();
            const PropertyTypeMap& propTypes = reader.getMetadata().propTypes();

            if (data._inputIdx >= inputNodeIDs->size()) {
                proc->finish();
                break;
            }

            const NodeID nodeID = (*inputNodeIDs)[data._inputIdx];
            const NodeView node = reader.getNodeView(nodeID);
            if (!node.isValid()) {
                throw ProcedureException(fmt::format("db.nodeProperties: invalid node ID: {}",
                                                     nodeID.getValue()));
            }

            for (const PropertyView& propView : node.properties()) {
                const auto name = propTypes.getName(propView._id);
                if (!name) {
                    continue;
                }

                if (outNodeIDCol) {
                    outNodeIDCol->push_back(nodeID);
                }
                if (outNameCol) {
                    outNameCol->push_back(name.value());
                }
                if (outValueCol) {
                    outValueCol->push_back(formatValue(propView));
                }
            }

            data._inputIdx++;

            if (data._inputIdx >= inputNodeIDs->size()) {
                proc->finish();
            }
        }
        break;
    }
}
