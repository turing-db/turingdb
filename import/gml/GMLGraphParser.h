#pragma once

#include <variant>

#include "writers/DataPartBuilder.h"
#include "GMLParser.h"
#include "ControlCharacters.h"
#include "Graph.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Change.h"
#include "writers/MetadataBuilder.h"

namespace db {

class GMLGraphSax {
public:
    // Variant to store different edge property value types for buffering
    using EdgePropValue = std::variant<std::string, uint64_t, int64_t, double>;

    struct BufferedEdgeProp {
        std::string key;
        EdgePropValue value;
    };

    explicit GMLGraphSax(Graph* graph)
        : _graph(graph)
    {
    }

    bool prepare() {
        _change = _graph->newChange();
        _commitBuilder = _change->access().getTip();
        _builder = &_commitBuilder->newBuilder();
        _metadata = &_commitBuilder->metadata();

        const LabelID labelID = _metadata->getOrCreateLabel("GMLNode");
        _labelset = _metadata->getOrCreateLabelSet(LabelSet::fromList({labelID}));
        _edgeTypeID = _metadata->getOrCreateEdgeType("GMLEdge");

        return true;
    }

    bool finish(JobSystem& jobs) {
        return _change->access().submit(jobs).has_value();
    }

    bool onNodeProperty(std::string_view k, std::string_view v) {
        _currentPropName = k;
        _currentPropName += " (String)";

        const auto propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::String);

        std::string escaped;
        ControlCharactersEscaper::escape(v, escaped);

        _builder->addNodeProperty<types::String>(_currentNodeID, propType._id, std::move(escaped));
        return true;
    }

    bool onNodeProperty(std::string_view k, uint64_t v) {
        _currentPropName = k;
        _currentPropName += " (UInt64)";

        const auto propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::UInt64);

        _builder->addNodeProperty<types::UInt64>(_currentNodeID, propType._id, v);
        return true;
    }

    bool onNodeProperty(std::string_view k, int64_t v) {
        _currentPropName = k;
        _currentPropName += " (Int64)";

        const auto propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::Int64);

        _builder->addNodeProperty<types::Int64>(_currentNodeID, propType._id, v);
        return true;
    }

    bool onNodeProperty(std::string_view k, double v) {
        _currentPropName = k;
        _currentPropName += " (Double)";

        const auto propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::Double);

        // TODO Handle error, if the property was already added
        _builder->addNodeProperty<types::Double>(_currentNodeID, propType._id, v);
        return true;
    }

    bool onNodeID(uint64_t id) {
        // TODO Handle errors, if we already encountered the property
        if (_nodeIDMapper.contains(id)) {
            return false;
        }
        _nodeIDMapper[id] = _currentNodeID;
        return true;
    }

    bool onEdgeProperty(std::string_view k, std::string_view v) {
        std::string escaped;
        ControlCharactersEscaper::escape(v, escaped);

        // Buffer the property if edge hasn't been created yet
        if (!_currentEdge) {
            _bufferedEdgeProps.push_back({std::string(k), std::move(escaped)});
            return true;
        }

        _currentPropName = k;
        _currentPropName += " (String)";

        const auto propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::String);
        _builder->addEdgeProperty<types::String>(*_currentEdge, propType._id, std::move(escaped));
        return true;
    }
    // TODO Add booleans

    bool onEdgeProperty(std::string_view k, uint64_t v) {
        // Buffer the property if edge hasn't been created yet
        if (!_currentEdge) {
            _bufferedEdgeProps.push_back({std::string(k), v});
            return true;
        }

        _currentPropName = k;
        _currentPropName += " (UInt64)";

        const auto propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::UInt64);
        _builder->addEdgeProperty<types::UInt64>(*_currentEdge, propType._id, v);
        return true;
    }

    bool onEdgeProperty(std::string_view k, int64_t v) {
        // Buffer the property if edge hasn't been created yet
        if (!_currentEdge) {
            _bufferedEdgeProps.push_back({std::string(k), v});
            return true;
        }

        _currentPropName = k;
        _currentPropName += " (Int64)";

        const auto propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::Int64);
        _builder->addEdgeProperty<types::Int64>(*_currentEdge, propType._id, v);
        return true;
    }

    bool onEdgeProperty(std::string_view k, double v) {
        // Buffer the property if edge hasn't been created yet
        if (!_currentEdge) {
            _bufferedEdgeProps.push_back({std::string(k), v});
            return true;
        }

        _currentPropName = k;
        _currentPropName += " (Double)";

        const auto propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::Double);
        _builder->addEdgeProperty<types::Double>(*_currentEdge, propType._id, v);
        return true;
    }

    bool onEdgeSource(uint64_t id) {
        auto nodeID = _nodeIDMapper.find(id);
        if (nodeID == _nodeIDMapper.end()) {
            return false;
        }

        _edgeSourceID = nodeID->second;
        if (_edgeTargetID.isValid()) {
            _currentEdge = &_builder->addEdge(_edgeTypeID, _edgeSourceID, _edgeTargetID);
            applyBufferedEdgeProperties();
        }
        return true;
    }

    bool onEdgeTarget(uint64_t id) {
        auto nodeID = _nodeIDMapper.find(id);
        if (nodeID == _nodeIDMapper.end()) {
            return false;
        }

        _edgeTargetID = nodeID->second;
        if (_edgeSourceID.isValid()) {
            _currentEdge = &_builder->addEdge(_edgeTypeID, _edgeSourceID, _edgeTargetID);
            applyBufferedEdgeProperties();
        }
        return true;
    }

    bool onNodeBegin() {
        _currentNodeID = _builder->addNode(_labelset);
        return true;
    }

    bool onNodeEnd() { return true; }

    bool onEdgeBegin() {
        _currentEdge = nullptr;
        _edgeSourceID = NodeID {};
        _edgeTargetID = NodeID {};
        _bufferedEdgeProps.clear();
        return true;
    }

    bool onEdgeEnd() { return true; }

private:
    void applyBufferedEdgeProperties() {
        for (const auto& prop : _bufferedEdgeProps) {
            std::visit([this, &prop](auto&& val) {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, std::string>) {
                    _currentPropName = prop.key;
                    _currentPropName += " (String)";
                    const auto propType = _metadata->getOrCreatePropertyType(
                        _currentPropName, ValueType::String);
                    _builder->addEdgeProperty<types::String>(
                        *_currentEdge, propType._id, std::string(val));
                } else if constexpr (std::is_same_v<T, uint64_t>) {
                    _currentPropName = prop.key;
                    _currentPropName += " (UInt64)";
                    const auto propType = _metadata->getOrCreatePropertyType(
                        _currentPropName, ValueType::UInt64);
                    _builder->addEdgeProperty<types::UInt64>(
                        *_currentEdge, propType._id, val);
                } else if constexpr (std::is_same_v<T, int64_t>) {
                    _currentPropName = prop.key;
                    _currentPropName += " (Int64)";
                    const auto propType = _metadata->getOrCreatePropertyType(
                        _currentPropName, ValueType::Int64);
                    _builder->addEdgeProperty<types::Int64>(
                        *_currentEdge, propType._id, val);
                } else if constexpr (std::is_same_v<T, double>) {
                    _currentPropName = prop.key;
                    _currentPropName += " (Double)";
                    const auto propType = _metadata->getOrCreatePropertyType(
                        _currentPropName, ValueType::Double);
                    _builder->addEdgeProperty<types::Double>(
                        *_currentEdge, propType._id, val);
                }
            }, prop.value);
        }
        _bufferedEdgeProps.clear();
    }

    Graph* _graph {nullptr};
    std::unique_ptr<Change> _change;
    CommitBuilder* _commitBuilder {nullptr};
    MetadataBuilder* _metadata {nullptr};
    DataPartBuilder* _builder {nullptr};
    LabelSetHandle _labelset;
    NodeID _currentNodeID;
    NodeID _edgeSourceID;
    NodeID _edgeTargetID;
    EdgeTypeID _edgeTypeID;
    const EdgeRecord* _currentEdge {nullptr};
    std::string _currentPropName;
    std::unordered_map<uint64_t, NodeID> _nodeIDMapper;
    std::vector<BufferedEdgeProp> _bufferedEdgeProps;
};

using GMLGraphParser = GMLParser<GMLGraphSax>;

}

