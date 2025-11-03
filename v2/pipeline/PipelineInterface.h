#pragma once

#include "PipelinePort.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIndices.h"

namespace db::v2 {

class PipelineInputPort;
class PipelineOutputPort;

class PipelineInputInterface {
public:
    PipelineInputPort* getPort() const { return _port; }
    Dataframe* getDataframe() const { return _port->getBuffer()->getDataframe(); }

    void setPort(PipelineInputPort* port) { _port = port; }

protected:
    PipelineInputPort* _port {nullptr};

    PipelineInputInterface() = default;
    virtual ~PipelineInputInterface() = default;
};

class PipelineBlockInputInterface : public PipelineInputInterface {
public:
};

class PipelineNodeInputInterface : public PipelineInputInterface {
public:
    ColumnNodeIDs* getNodeIDs() const { return _nodeIDs; }

    void setNodeIDs(ColumnNodeIDs* nodeIDs) { _nodeIDs = nodeIDs; }

private:
    ColumnNodeIDs* _nodeIDs {nullptr};
};

class PipelineEdgeInputInterface : public PipelineInputInterface {
public:
    void setEdges(ColumnEdgeIDs* edgeIDs,
                  ColumnNodeIDs* targetNodes,
                  ColumnEdgeTypes* edgeTypes) {
        _edgeIDs = edgeIDs;
        _targetNodes = targetNodes;
        _edgeTypes = edgeTypes;
    }

    ColumnEdgeIDs* getEdgeIDs() const { return _edgeIDs; }
    ColumnNodeIDs* getTargetNodes() const { return _targetNodes; }
    ColumnEdgeTypes* getEdgeTypes() const { return _edgeTypes; }

private:
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _targetNodes {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
};

class PipelineOutputInterface {
public:
    PipelineOutputPort* getPort() const { return _port; }
    Dataframe* getDataframe() const { return _port->getBuffer()->getDataframe(); }

    void setPort(PipelineOutputPort* port) { _port = port; }

protected:
    PipelineOutputPort* _port {nullptr};

    PipelineOutputInterface() = default;
    virtual ~PipelineOutputInterface() = default;
};

class PipelineBlockOutputInterface : public PipelineOutputInterface {
public:
    void connectTo(PipelineBlockInputInterface& input) {
        _port->connectTo(input.getPort());
    }
};

class PipelineNodeOutputInterface : public PipelineOutputInterface {
public:
    void setNodeIDs(ColumnNodeIDs* nodeIDs) { _nodeIDs = nodeIDs; }
    void setIndices(ColumnIndices* indices) { _indices = indices; }

    ColumnIndices* getIndices() const { return _indices; }

    ColumnNodeIDs* getNodeIDs() const { return _nodeIDs; }

    void connectTo(PipelineNodeInputInterface& input) {
        input.setNodeIDs(_nodeIDs);
        _port->connectTo(input.getPort());
    }

    void connectTo(PipelineBlockInputInterface& input) {
        _port->connectTo(input.getPort());
    }

private:
    ColumnIndices* _indices {nullptr};
    ColumnNodeIDs* _nodeIDs {nullptr};
};

class PipelineEdgeOutputInterface : public PipelineOutputInterface {
public:
    void setEdges(ColumnEdgeIDs* edgeIDs,
                  ColumnNodeIDs* targetNodes,
                  ColumnEdgeTypes* edgeTypes) {
        _edgeIDs = edgeIDs;
        _targetNodes = targetNodes;
        _edgeTypes = edgeTypes;
    }

    void setIndices(ColumnIndices* indices) { _indices = indices; }

    ColumnIndices* getIndices() const { return _indices; }

    ColumnEdgeIDs* getEdgeIDs() const { return _edgeIDs; }
    ColumnNodeIDs* getTargetNodes() const { return _targetNodes; }
    ColumnEdgeTypes* getEdgeTypes() const { return _edgeTypes; }

    void connectTo(PipelineEdgeInputInterface& input) {
        input.setEdges(_edgeIDs, _targetNodes, _edgeTypes);
        _port->connectTo(input.getPort());
    }

    void connectTo(PipelineNodeInputInterface& input) {
        input.setNodeIDs(_targetNodes);
        _port->connectTo(input.getPort());
    }

    void connectTo(PipelineBlockInputInterface& input) {
        _port->connectTo(input.getPort());
    }

private:
    ColumnIndices* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnNodeIDs* _targetNodes {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
};

template <typename ValueType>
class PipelineValuesOutputInterface : public PipelineOutputInterface {
public:
    using ColumnValues = ColumnVector<ValueType>;

    void setIndices(ColumnIndices* indices) { _indices = indices; }
    void setValues(ColumnValues* values) { _values = values; }

    ColumnIndices* getIndices() const { return _indices; }
    ColumnValues* getValues() const { return _values; }

    void connectTo(PipelineBlockInputInterface& input) {
        _port->connectTo(input.getPort());
    }

private:
    ColumnIndices* _indices {nullptr};
    ColumnValues* _values {nullptr};
};

template <typename ValueType>
class PipelineValueOutputInterface : public PipelineOutputInterface {
public:
    using ColumnConstValue = ColumnConst<ValueType>;

    void setIndices(ColumnIndices* indices) { _indices = indices; }
    void setValue(ColumnConstValue* value) { _value = value; }

    ColumnIndices* getIndices() const { return _indices; }
    ColumnConstValue* getValue() const { return _value; }

    void connectTo(PipelineBlockInputInterface& input) {
        _port->connectTo(input.getPort());
    }

private:
    ColumnIndices* _indices {nullptr};
    ColumnConstValue* _value {nullptr};
};

}
