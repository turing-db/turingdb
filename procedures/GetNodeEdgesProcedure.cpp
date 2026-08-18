#include "GetNodeEdgesProcedure.h"

#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "ProcedureContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "ProcUtils.h"
#include "ProcedureException.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnIDs.h"
#include "views/GraphView.h"
#include "views/NodeView.h"
#include "reader/GraphReader.h"
#include "datapart/EdgeRecord.h"
#include "versioning/Tombstones.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "ID.h"

using namespace db;

namespace {

using NodeIDCol = ColumnVector<NodeID>;
using ListColumn = ColumnVector<ListView>;
using StringColumn = ColumnVector<std::string>;
using ItemVariant = ListBuffer<>::ListItemVariant;
using LimitMap = std::unordered_map<EdgeTypeID, size_t>;
// Ordered so the JSON emitted by countsToJson has a deterministic key order
// (by edge type id) across stdlib implementations.
using CountMap = std::map<EdgeTypeID, size_t>;

using Int64Type = types::Int64::Primitive;
using BoolType = types::Bool::Primitive;

constexpr std::string_view nodeIDsErr = "getNodeEdges: nodeIDs must be a constant list";
constexpr std::string_view edgeTypeListErr = "getNodeEdges: edge-type list must be a constant list";
constexpr std::string_view limitValueListErr = "getNodeEdges: limit-value list must be a constant list";
constexpr std::string_view defaultLimitErr = "getNodeEdges: defaultLimit must be a constant int";
constexpr std::string_view returnOnlyIDsErr = "getNodeEdges: returnOnlyIDs must be a constant bool";

struct Data : public ProcedureData {};

// Fill `limits` with an {edgeTypeID -> limit} map from parallel (types, values)
// list args.
void buildLimitMap(const Column* typesArg, const Column* valuesArg, LimitMap& limits) {
    limits.clear();
    std::vector<int64_t> types;
    std::vector<int64_t> values;
    const auto& edgeTypesList = ProcUtils::constArg<ListView>(typesArg, edgeTypeListErr);
    ProcUtils::readIntList(&edgeTypesList, types);
    const auto& valuesList = ProcUtils::constArg<ListView>(valuesArg, limitValueListErr);
    ProcUtils::readIntList(&valuesList, values);

    for (size_t i = 0; i < types.size() && i < values.size(); ++i) {
        if (types[i] < 0) {
            throw ProcedureException("Edge type id cannot be negative");
        }
        if (values[i] < 0) {
            throw ProcedureException("Limit value cannot be negative");
        }
        const auto idType = static_cast<EdgeTypeID::Type>(types[i]);
        limits[EdgeTypeID {idType}] = static_cast<size_t>(values[i]);
    }
}

// JSON-encode an {edgeTypeID -> count} map into `out` as {"typeId": count, ...}.
void countsToJson(const CountMap& counts, std::string& out) {
    out.clear();
    out += '{';
    bool first = true;
    for (const auto& [type, count] : counts) {
        if (!first) {
            out += ',';
        }
        first = false;
        out += '"';
        out += std::to_string(type.getValue());
        out += "\":";
        out += std::to_string(count);
    }
    out += '}';
}

void executeImpl(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const Column* inputNodeIDs = data.getInputColumn(0);
    const Column* inputLimit = data.getInputColumn(1);
    const Column* inputOutTypes = data.getInputColumn(2);
    const Column* inputOutValues = data.getInputColumn(3);
    const Column* inputInTypes = data.getInputColumn(4);
    const Column* inputInValues = data.getInputColumn(5);
    const Column* inputReturnOnlyIds= data.getInputColumn(6);

    auto* idCol = static_cast<NodeIDCol*>(data.getReturnColumn(0));
    auto* outCol = static_cast<ListColumn*>(data.getReturnColumn(1));
    auto* inCol = static_cast<ListColumn*>(data.getReturnColumn(2));
    auto* outCountsCol = static_cast<StringColumn*>(data.getReturnColumn(3));
    auto* inCountsCol = static_cast<StringColumn*>(data.getReturnColumn(4));

    const GraphView& view = *ctxt->getGraphView();
    const GraphReader reader(view);
    ListBuffer<>* listBuffer = ctxt->getListBuffer();

    const bool hasNodeTombstones = view.tombstones().hasNodes();
    const bool hasEdgeTombstones = view.tombstones().hasEdges();

    std::vector<int64_t> nodeIDs;
    const auto& nodeIDList = ProcUtils::constArg<ListView>(inputNodeIDs, nodeIDsErr);
    ProcUtils::readIntList(&nodeIDList, nodeIDs);

    const int64_t signedDefaultLimit = ProcUtils::constArg<Int64Type>(inputLimit, defaultLimitErr);
    if (signedDefaultLimit < 0) {
        throw ProcedureException("Default limit cannot be negative");
    }
    const size_t defaultLimit = static_cast<size_t>(signedDefaultLimit);

    const bool returnOnlyIDs = ProcUtils::constArg<BoolType>(inputReturnOnlyIds, returnOnlyIDsErr);

    LimitMap outLimits;
    buildLimitMap(inputOutTypes, inputOutValues, outLimits);
    LimitMap inLimits;
    buildLimitMap(inputInTypes, inputInValues, inLimits);

    if (idCol) {
        idCol->clear();
    }
    if (outCol) {
        outCol->clear();
    }
    if (inCol) {
        inCol->clear();
    }
    if (outCountsCol) {
        outCountsCol->clear();
    }
    if (inCountsCol) {
        inCountsCol->clear();
    }

    ColumnNodeIDs singleNodeID(1);
    std::vector<ItemVariant> edgeItems;
    std::vector<ItemVariant> outerItems;

    // Build the nested-list column value for one direction's edges,
    // truncating per edge type; fills `counts` with the per-type totals
    // (counting ALL edges, including those past the limit).
    const auto buildEdges = [&](auto&& edgeRange, const LimitMap& limits,
                                bool isOut, CountMap& counts) -> ListView {
        counts.clear();
        outerItems.clear();
        for (const EdgeRecord& e : edgeRange) {
            // Base edge iterators don't filter tombstones; skip deleted edges so
            // they leak into neither the emitted list nor the per-type counts.
            if (hasEdgeTombstones && reader.edgeIsDeleted(e._edgeID)) {
                continue;
            }
            size_t& seen = counts[e._edgeTypeID];

            const auto limitIter = limits.find(e._edgeTypeID);
            const size_t limit = limitIter != limits.end()
                                   ? limitIter->second
                                   : defaultLimit;

            ++seen;
            if (seen > limit) {
                continue;
            }

            edgeItems.clear();
            edgeItems.emplace_back(static_cast<int64_t>(e._edgeID.getValue()));
            if (returnOnlyIDs) {
                edgeItems.emplace_back(static_cast<int64_t>(e._otherID.getValue()));
            } else {
                const int64_t src = isOut
                                      ? static_cast<int64_t>(e._nodeID.getValue())
                                      : static_cast<int64_t>(e._otherID.getValue());
                const int64_t tgt = isOut
                                      ? static_cast<int64_t>(e._otherID.getValue())
                                      : static_cast<int64_t>(e._nodeID.getValue());
                edgeItems.emplace_back(src);
                edgeItems.emplace_back(tgt);
                edgeItems.emplace_back(static_cast<int64_t>(e._edgeTypeID.getValue()));
            }
            outerItems.emplace_back(listBuffer->insert(edgeItems));
        }
        return listBuffer->insert(outerItems);
    };

    CountMap outCounts;
    CountMap inCounts;
    std::string countsJson;

    for (const int64_t rawID : nodeIDs) {
        const NodeID nodeID {static_cast<NodeID::Type>(rawID)};
        if (hasNodeTombstones && reader.nodeIsDeleted(nodeID)) {
            continue; // deleted node
        }
        const NodeView node = reader.getNodeView(nodeID);
        if (!node.isValid()) {
            throw ProcedureException("Invalid node ID: " + std::to_string(rawID));
        }
        singleNodeID[0] = nodeID;

        const ListView outgoingList =
            buildEdges(reader.getOutEdges(&singleNodeID), outLimits, true, outCounts);
        const ListView incomingList =
            buildEdges(reader.getInEdges(&singleNodeID), inLimits, false, inCounts);

        if (idCol) {
            idCol->push_back(nodeID);
        }
        if (outCol) {
            outCol->push_back(outgoingList);
        }
        if (inCol) {
            inCol->push_back(incomingList);
        }
        if (outCountsCol) {
            countsToJson(outCounts, countsJson);
            outCountsCol->push_back(countsJson);
        }
        if (inCountsCol) {
            countsToJson(inCounts, countsJson);
            inCountsCol->push_back(countsJson);
        }
    }

    proc->finish();
}

}

ProcedureData* GetNodeEdgesProcedure::allocData() {
    return new Data();
}

void GetNodeEdgesProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void GetNodeEdgesProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("getNodeEdges");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addConstantArgument("nodeIDs", ProcedureType::LIST);
    proc->addConstantArgument("defaultLimit", ProcedureType::INT64);
    proc->addConstantArgument("outLimitTypes", ProcedureType::LIST);
    proc->addConstantArgument("outLimitValues", ProcedureType::LIST);
    proc->addConstantArgument("inLimitTypes", ProcedureType::LIST);
    proc->addConstantArgument("inLimitValues", ProcedureType::LIST);
    proc->addConstantArgument("returnOnlyIDs", ProcedureType::BOOL);
    proc->addReturnValue("id", ProcedureType::NODE);
    proc->addReturnValue("outgoingEdges", ProcedureType::LIST);
    proc->addReturnValue("incomingEdges", ProcedureType::LIST);
    proc->addReturnValue("outEdgeCounts", ProcedureType::STRING);
    proc->addReturnValue("inEdgeCounts", ProcedureType::STRING);
    ns->addProcedure(proc);
}

void GetNodeEdgesProcedure::execute(ProcedureState* proc) {
    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        executeImpl(proc);
        break;
    }
}
