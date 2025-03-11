#pragma once

#include "writers/DataPartBuilder.h"
#include "GMLParser.h"
#include "ControlCharacters.h"
#include "Graph.h"
#include "versioning/CommitBuilder.h"

namespace db {

class GMLGraphSax {
public:
    explicit GMLGraphSax(Graph* graph)
        : _graph(graph)
    {
    }

    bool prepare() {
        _propTypes = &_graph->getMetadata()->propTypes();

        const LabelID labelID = _graph->getMetadata()->labels().create("GMLNode");
        const LabelSet labelset = LabelSet::fromList({labelID});
        _labelsetID = _graph->getMetadata()->labelsets().create(labelset);

        _edgeTypeID = _graph->getMetadata()->edgeTypes().create("GMLEdge");
        const auto tx = _graph->openWriteTransaction();
        _commitBuilder = tx.prepareCommit();
        _builder = &_commitBuilder->newBuilder();
        return true;
    }

    void finish(JobSystem& jobs) {
        _graph->commit(std::move(_commitBuilder), jobs);
    }

    bool onNodeProperty(std::string_view k, std::string_view v) {
        _currentPropName = k;
        _currentPropName += " (String)";

        const auto propType = _propTypes->getOrCreate(_currentPropName, ValueType::String);

        std::string escaped;
        ControlCharactersEscaper::escape(v, escaped);

        _builder->addNodeProperty<types::String>(_currentNodeID, propType._id, std::move(escaped));
        return true;
    }

    bool onNodeProperty(std::string_view k, uint64_t v) {
        _currentPropName = k;
        _currentPropName += " (UInt64)";

        const auto propType = _propTypes->getOrCreate(_currentPropName, ValueType::UInt64);

        _builder->addNodeProperty<types::UInt64>(_currentNodeID, propType._id, v);
        return true;
    }

    bool onNodeProperty(std::string_view k, int64_t v) {
        _currentPropName = k;
        _currentPropName += " (Int64)";

        const auto propType = _propTypes->getOrCreate(_currentPropName, ValueType::Int64);

        _builder->addNodeProperty<types::Int64>(_currentNodeID, propType._id, v);
        return true;
    }

    bool onNodeProperty(std::string_view k, double v) {
        _currentPropName = k;
        _currentPropName += " (Double)";

        const auto propType = _propTypes->getOrCreate(_currentPropName, ValueType::Double);

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
        _currentPropName = k;
        _currentPropName += " (String)";

        const auto propType = _propTypes->getOrCreate(_currentPropName, ValueType::String);

        std::string escaped;
        ControlCharactersEscaper::escape(v, escaped);

        _builder->addEdgeProperty<types::String>(*_currentEdge, propType._id, std::move(escaped));
        return true;
    }
    // TODO Add booleans

    bool onEdgeProperty(std::string_view k, uint64_t v) {
        _currentPropName = k;
        _currentPropName += " (UInt64)";

        const auto propType = _propTypes->getOrCreate(_currentPropName, ValueType::UInt64);

        _builder->addEdgeProperty<types::UInt64>(*_currentEdge, propType._id, v);
        return true;
    }

    bool onEdgeProperty(std::string_view k, int64_t v) {
        _currentPropName = k;
        _currentPropName += " (Int64)";

        const auto propType = _propTypes->getOrCreate(_currentPropName, ValueType::Int64);

        _builder->addEdgeProperty<types::Int64>(*_currentEdge, propType._id, v);
        return true;
    }

    bool onEdgeProperty(std::string_view k, double v) {
        _currentPropName = k;
        _currentPropName += " (Double)";

        const auto propType = _propTypes->getOrCreate(_currentPropName, ValueType::Double);

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
        }
        return true;
    }

    bool onNodeBegin() {
        _currentNodeID = _builder->addNode(_labelsetID);
        return true;
    }

    bool onNodeEnd() { return true; }

    bool onEdgeBegin() {
        _currentEdge = nullptr;
        _edgeSourceID = EntityID();
        _edgeTargetID = EntityID();
        return true;
    }

    bool onEdgeEnd() { return true; }

private:
    Graph* _graph {nullptr};
    PropertyTypeMap* _propTypes {nullptr};

    std::unique_ptr<CommitBuilder> _commitBuilder;
    DataPartBuilder* _builder {nullptr};
    LabelSetID _labelsetID;
    EntityID _currentNodeID;
    EntityID _edgeSourceID;
    EntityID _edgeTargetID;
    EdgeTypeID _edgeTypeID;
    const EdgeRecord* _currentEdge {nullptr};
    std::string _currentPropName;
    std::unordered_map<uint64_t, EntityID> _nodeIDMapper;
};

using GMLGraphParser = GMLParser<GMLGraphSax>;

}

