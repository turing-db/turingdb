#include "ListNodesProcedure.h"

#include <cctype>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
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
#include "views/EntityPropertyView.h"
#include "reader/GraphReader.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelMap.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyTypeMap.h"
#include "metadata/PropertyType.h"
#include "versioning/Tombstones.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "list/ListElementView.h"
#include "list/ListBufferTypeTag.h"
#include "map/MapBufferTypeTag.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"
#include "ID.h"

using namespace db;

namespace {

using NodeIDCol = ColumnVector<NodeID>;
using ListColumn = ColumnVector<ListView>;
using StringColumn = ColumnVector<std::string>;

constexpr std::string_view labelsErr = "listNodes: labels must be a constant list";
constexpr std::string_view propertiesErr = "listNodes: properties must be a constant map";
constexpr std::string_view skipErr = "listNodes: skip must be a constant int";
constexpr std::string_view limitErr = "listNodes: limit must be a constant int";

struct Data : public ProcedureData {};

struct PropertyFilter {
    PropertyTypeID _id;
    std::string _query; // lower-cased substring query
};

void toLowerInto(std::string_view src, std::string& dst) {
    dst.resize(src.size());
    for (size_t i = 0; i < src.size(); ++i) {
        dst[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(src[i])));
    }
}

// Read the string elements of `view` into `out`; non-string elements are
// skipped. The caller supplies the list view via `ProcUtils::constArg<ListView>`,
// which enforces the constant-list rule.
void readStringList(const ListView* view, std::vector<std::string>& out) {
    for (const ListElementView& el : *view) {
        if (el.getTag() != ListBufferTypeTag::String) {
            continue;
        }
        out.emplace_back(el.getAs<std::string_view>());
    }
}

// Read `properties` into `out` as one filter per entry: the key names the property, the
// string value is the substring query, lower-cased for case-insensitive matching. Entries
// whose value is not a string, or that name an unknown or non-string property, are
// skipped. The caller supplies the map view via `ProcUtils::constArg<MapView>`, which
// enforces the constant-map rule.
void readPropertyFilters(const MapView* properties,
                         const PropertyTypeMap& propTypes,
                         std::vector<PropertyFilter>& out) {
    std::string lowered;

    for (const MapEntryView& entry : properties->entries()) {
        if (entry.getValueTag() != MapBufferTypeTag::String) {
            continue;
        }

        const std::optional<PropertyType> propType = propTypes.get(entry.getKey());
        if (!propType || propType.value()._valueType != ValueType::String) {
            continue;
        }

        toLowerInto(entry.getValueAs<std::string_view>(), lowered);
        out.push_back({propType.value()._id, std::move(lowered)});
    }
}

bool checkRemainingFilters(const NodeView node,
                           std::span<PropertyFilter> filters,
                           std::string& lowerCaseString) {
    for (const auto& filter : filters) {
        const auto* value =
            node.properties().tryGetProperty<types::String>(filter._id);
        if (!value) {
            return false;
        }
        toLowerInto(*value, lowerCaseString);
        if (lowerCaseString.find(filter._query) == std::string::npos) {
            return false;
        }
    }

    return true;
}

void executeImpl(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const Column* inputLabelNames = data.getInputColumn(0);
    const Column* inputProperties = data.getInputColumn(1);

    auto* idCol = static_cast<NodeIDCol*>(data.getReturnColumn(0));
    auto* labelsCol = static_cast<ListColumn*>(data.getReturnColumn(1));
    auto* propsCol = static_cast<StringColumn*>(data.getReturnColumn(2));

    const GraphView& view = *ctxt->getGraphView();
    const GraphReader reader(view);
    const GraphMetadata& metadata = reader.getMetadata();
    const LabelMap& labelMap = metadata.labels();
    const PropertyTypeMap& propTypes = metadata.propTypes();
    ListBuffer<4096>* listBuffer = ctxt->getListBuffer();

    const bool hasTombstones = view.hasDeletedNodes();

    // --- Arguments -------------------------------------------------
    std::vector<std::string> labelNames;

    const auto& labelsList = ProcUtils::constArg<ListView>(inputLabelNames, labelsErr);
    readStringList(&labelsList, labelNames);

    const auto& propertiesMap = ProcUtils::constArg<MapView>(inputProperties, propertiesErr);

    const int64_t skipArg = ProcUtils::constArg<types::Int64::Primitive>(data.getInputColumn(2), skipErr);
    const int64_t limitArg = ProcUtils::constArg<types::Int64::Primitive>(data.getInputColumn(3), limitErr);

    if (skipArg < 0) {
        throw ProcedureException("Skip argument cannot be negative");
    }
    const size_t skip = static_cast<size_t>(skipArg);

    if (limitArg < 0) {
        throw ProcedureException("Limit argument cannot be negative");
    }
    const size_t limit = static_cast<size_t>(limitArg);

    // Label filter -> label set (unknown labels ignored)
    LabelSet labelset;
    for (const std::string& name : labelNames) {
        const auto id = labelMap.get(name);
        if (id) {
            labelset.set(id.value());
        }
    }

    // Property filters: string properties only, queries lower-cased for
    // case-insensitive substring matching.
    std::vector<PropertyFilter> filters;
    readPropertyFilters(&propertiesMap, propTypes, filters);

    if (idCol) {
        idCol->clear();
    }
    if (labelsCol) {
        labelsCol->clear();
    }
    if (propsCol) {
        propsCol->clear();
    }

    const size_t cap = skip + limit;

    std::string lowerValue;
    std::string propsJson;
    std::vector<LabelID> labelIDs;
    std::vector<ListBuffer<>::ListItemVariant> items;

    // add a matching node's view, honouring skip/limit.
    // `matched` counts nodes passing the filters; returns false to stop.
    size_t matched = 0;
    const auto addNode = [&](const NodeView& node) -> bool {
        if (matched == cap) {
            return false; // page filled; stop
        }
        ++matched;
        if (matched <= skip) {
            return true; // inside skip window
        }

        // Valid node - push back values to column
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

        if (propsCol) {
            ProcUtils::encodeProperties(node.properties(), propTypes, propsJson);
            propsCol->push_back(propsJson);
        }

        return true;
    };

    // Deleted nodes still surface in the scans below (the base iterators
    // don't filter tombstones); skip them by id before building a view.
    const auto isDeleted = [&](const NodeID id) -> bool {
        return hasTombstones && reader.nodeIsDeleted(id);
    };

    if (!filters.empty()) {
        // We will get scan nodes for the first property in our filters vector
        // and check the values against the property values, then get the
        // NodeView and then check it against the rest of the filters
        const PropertyFilter& first = filters.front();
        const auto runScan = [&](auto&& range) {
            for (auto it = range.begin(); it.isValid(); it.next()) {
                toLowerInto(it.get(), lowerValue);
                if (lowerValue.find(first._query) == std::string::npos) {
                    continue;
                }
                const NodeID nodeID = it.getCurrentNodeID();
                if (isDeleted(nodeID)) {
                    continue;
                }
                const NodeView node = reader.getNodeView(nodeID);
                auto remainingFilters = std::span<PropertyFilter>(filters).subspan(1);
                if (!checkRemainingFilters(node, remainingFilters, lowerValue)) {
                    continue;
                }
                if (!addNode(node)) {
                    return;
                }
            }
        };
        if (labelset.empty()) {
            runScan(reader.scanNodeProperties<types::String>(first._id));
        } else {
            runScan(reader.scanNodePropertiesByLabel<types::String>(
                first._id, LabelSetHandle {labelset}));
        }
    } else if (!labelset.empty()) {
        for (const NodeID nodeID : reader.scanNodesByLabel(LabelSetHandle {labelset})) {
            if (isDeleted(nodeID)) {
                continue;
            }
            if (!addNode(reader.getNodeView(nodeID))) {
                break;
            }
        }
    } else {
        for (const NodeID nodeID : reader.scanNodes()) {
            if (isDeleted(nodeID)) {
                continue;
            }
            if (!addNode(reader.getNodeView(nodeID))) {
                break;
            }
        }
    }

    proc->finish();
}

}

ProcedureData* ListNodesProcedure::allocData() {
    return new Data();
}

void ListNodesProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void ListNodesProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("listNodes");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addConstantArgument("labels", ProcedureType::LIST);
    proc->addConstantArgument("properties", ProcedureType::MAP);
    proc->addConstantArgument("skip", ProcedureType::INT64);
    proc->addConstantArgument("limit", ProcedureType::INT64);
    proc->addReturnValue("id", ProcedureType::NODE);
    proc->addReturnValue("labels", ProcedureType::LIST);
    proc->addReturnValue("properties", ProcedureType::STRING);
    ns->addProcedure(proc);
}

void ListNodesProcedure::execute(ProcedureState* proc) {
    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        executeImpl(proc);
        break;
    }
}
