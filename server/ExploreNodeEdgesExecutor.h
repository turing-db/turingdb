#pragma once

#include <stddef.h>
#include <unordered_set>

#include "PayloadWriter.h"
#include "reader/GraphReader.h"
#include "ID.h"

#define CASE_RUN_FILTER(filters_set)               \
    case combineFilters filters_set:               \
        runFiltered<combineFilters filters_set>(); \
        break;


namespace db {

class ExploreNodeEdgesExecutor {
public:
    struct PropertyFilter {
        PropertyTypeID _pID;
        std::string _query;
    };

    ExploreNodeEdgesExecutor(const GraphReader& reader, PayloadWriter& w)
        : _w(w),
        _reader(reader),
        _propTypes(_reader.getMetadata().propTypes()),
        _labelsets(_reader.getMetadata().labelsets()),
        _nodeID(1)
    {
    }

    enum Filters : uint64_t {
        NoFilter = 0,
        NodeProperties = 1 << 0,
        EdgeProperties = 1 << 1,
        Labels = 1 << 2,
        EdgeTypes = 1 << 3,
    };

    static consteval uint64_t combineFilters(std::initializer_list<Filters> filters) {
        uint64_t combined = 0;
        for (auto filter : filters) {
            combined |= (uint64_t)filter;
        }
        return combined;
    }

    void run() {
        if (_excludeOutgoing && _excludeincoming) {
            _w.key("error");
            _w.value("Cannot exclude both incoming and outgoing edges");
            return;
        }

        auto filters = (uint64_t)Filters::NoFilter;

        if (!_nodeProperties.empty()) {
            filters |= (uint64_t)Filters::NodeProperties;
        }

        if (!_edgeProperties.empty()) {
            filters |= (uint64_t)Filters::EdgeProperties;
        }

        if (!_labelset.empty()) {
            filters |= (uint64_t)Filters::Labels;
        }

        if (!_edgeTypeIDs.empty()) {
            filters |= (uint64_t)Filters::EdgeTypes;
        }

        _w.obj();

        switch (filters) {
            CASE_RUN_FILTER(({NoFilter}))
            CASE_RUN_FILTER(({NodeProperties}))
            CASE_RUN_FILTER(({NodeProperties, EdgeProperties}))
            CASE_RUN_FILTER(({NodeProperties, EdgeProperties, Labels}))
            CASE_RUN_FILTER(({NodeProperties, EdgeProperties, Labels, EdgeTypes}))
            CASE_RUN_FILTER(({EdgeProperties}))
            CASE_RUN_FILTER(({EdgeProperties, Labels}))
            CASE_RUN_FILTER(({EdgeProperties, Labels, EdgeTypes}))
            CASE_RUN_FILTER(({Labels}))
            CASE_RUN_FILTER(({Labels, EdgeTypes}))
            CASE_RUN_FILTER(({EdgeTypes}))

            default: {
                _w.key("error");
                _w.value("Invalid combination of filters");
                return;
            }
        }

        _w.end(); // data
    }

    void addNodePropertyFilter(PropertyTypeID id, std::string&& str) {
        _nodeProperties.emplace_back(id, std::move(str));
    }

    void addEdgePropertyFilter(PropertyTypeID id, std::string&& str) {
        _edgeProperties.emplace_back(id, std::move(str));
    }

    void addLabel(LabelID id) { _labelset.set(id); }
    void addEdgeType(EdgeTypeID id) { _edgeTypeIDs.emplace(id); }
    void setNodeID(NodeID id) { _nodeID[0] = id; }
    void setSkip(size_t skip) { _skip = skip; }
    void setLimit(size_t limit) { _limit = limit; }
    void setExcludeOutgoing(bool v) { _excludeOutgoing = v; }
    void setExcludeIncoming(bool v) { _excludeincoming = v; }

private:
    static constexpr size_t DEFAULT_LIMIT = 1000;
    PayloadWriter& _w;
    GraphReader _reader;
    const PropertyTypeMap& _propTypes;
    const LabelSetMap& _labelsets;

    size_t _skip {0};
    size_t _limit {DEFAULT_LIMIT};

    bool _excludeOutgoing = false;
    bool _excludeincoming = false;

    ColumnVector<NodeID> _nodeID;
    std::vector<PropertyFilter> _nodeProperties;
    std::vector<PropertyFilter> _edgeProperties;
    std::unordered_set<EdgeTypeID> _edgeTypeIDs;
    LabelSet _labelset;

    template <uint64_t FiltersT>
    bool matchesEdgeTypes(EdgeTypeID typeID) {
        if constexpr ((FiltersT & (uint64_t)Filters::EdgeTypes) != 0) {
            return _edgeTypeIDs.contains(typeID);
        }
        return true;
    }

    template <uint64_t FiltersT>
    bool matchesLabels(NodeID nodeID) {
        if constexpr ((FiltersT & (uint64_t)Filters::Labels) != 0) {
            const auto labelset = _reader.getNodeLabelSet(nodeID);
            if (!labelset.isValid()) {
                return false;
            }
            return labelset.hasAtLeastLabels(LabelSetHandle {_labelset});
        }
        return true;
    }

    template <uint64_t FiltersT>
    bool matchesNodeProperties(NodeID nodeID) {
        if constexpr ((FiltersT & (uint64_t)Filters::NodeProperties) != 0) {
            for (const auto& [pID, query] : _nodeProperties) {
                const auto* p = _reader.tryGetNodeProperty<types::String>(pID, nodeID);

                if (!p) {
                    return false;
                }

                const bool matches = std::search(
                                         p->begin(), p->end(),
                                         query.begin(), query.end(),
                                         [](char a, char b) { return std::tolower(a) == b; })
                                  != p->end();
                if (!matches) {
                    return false;
                }
            }
        }

        return true;
    }

    template <uint64_t FiltersT>
    bool matchesEdgeProperties(const EdgeView& edge) {
        if constexpr ((FiltersT & (uint64_t)Filters::EdgeProperties) != 0) {
            for (const auto& [pID, query] : _edgeProperties) {
                const auto* it = edge.properties().tryGetProperty<types::String>(pID);

                if (!it) {
                    return false;
                }

                const bool matches = std::search(
                                         it->begin(), it->end(),
                                         query.begin(), query.end(),
                                         [](char a, char b) { return std::tolower(a) == b; })
                                  != it->end();
                if (!matches) {
                    return false;
                }
            }
        }

        return true;
    }

    template <uint64_t FiltersT>
    void runFiltered() {
        if (!_excludeOutgoing) {
            size_t outCount = 0;
            _w.key("outs");
            _w.arr();

            for (const EdgeRecord& out : _reader.getOutEdges(&_nodeID)) {
                if (!matchesEdgeTypes<FiltersT>(out._edgeTypeID)) {
                    continue;
                }
                if (!matchesLabels<FiltersT>(out._otherID)) {
                    continue;
                }
                if (!matchesNodeProperties<FiltersT>(out._otherID)) {
                    continue;
                }

                const EdgeView outv = _reader.getEdgeView(out._edgeID);
                if (!matchesEdgeProperties<FiltersT>(outv)) {
                    continue;
                }

                outCount++;

                if (outCount <= _skip) {
                    continue;
                }

                if (outCount > _skip + _limit) {
                    continue;
                }

                _w.value(outv);
            }

            _w.end(); // outs
            _w.key("outCount");
            _w.value(outCount);
        }

        if (!_excludeincoming) {
            size_t inCount = 0;
            _w.key("ins");
            _w.arr();

            for (const EdgeRecord& in : _reader.getInEdges(&_nodeID)) {
                if (!matchesEdgeTypes<FiltersT>(in._edgeTypeID)) {
                    continue;
                }
                if (!matchesLabels<FiltersT>(in._otherID)) {
                    continue;
                }
                if (!matchesNodeProperties<FiltersT>(in._otherID)) {
                    continue;
                }

                const EdgeView inv = _reader.getEdgeView(in._edgeID);
                if (!matchesEdgeProperties<FiltersT>(inv)) {
                    continue;
                }

                inCount++;

                if (inCount <= _skip) {
                    continue;
                }

                if (inCount > _skip + _limit) {
                    continue;
                }

                _w.value(inv);
            }

            _w.end(); // ins
            _w.key("inCount");
            _w.value(inCount);
        }
    }
};

}
