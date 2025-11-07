#pragma once

#include "PipelinePort.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineInputPort;
class PipelineOutputPort;
class PipelineNodeInputInterface;
class PipelineBlockInputInterface;
class PipelineEdgeInputInterface;

enum class PipelineInterfaceKind {
    NODE,
    EDGE,
    BLOCK,
    VALUES,
    VALUE
};

class PipelineInputInterface {
public:
    virtual PipelineInterfaceKind getKind() const = 0;

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
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::BLOCK; }
};

class PipelineNodeInputInterface : public PipelineInputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::NODE; }

    NamedColumn* getNodeIDs() const { return _nodeIDs; }

    void setNodeIDs(NamedColumn* nodeIDs) { _nodeIDs = nodeIDs; }

private:
    NamedColumn* _nodeIDs {nullptr};
};

class PipelineEdgeInputInterface : public PipelineInputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::EDGE; }

    void setEdges(NamedColumn* edgeIDs,
                  NamedColumn* neighbourNodes,
                  NamedColumn* edgeTypes) {
        _edgeIDs = edgeIDs;
        _otherNodes = neighbourNodes;
        _edgeTypes = edgeTypes;
    }

    NamedColumn* getEdgeIDs() const { return _edgeIDs; }
    NamedColumn* getOtherNodes() const { return _otherNodes; }
    NamedColumn* getEdgeTypes() const { return _edgeTypes; }

private:
    NamedColumn* _edgeIDs {nullptr};
    NamedColumn* _otherNodes {nullptr};
    NamedColumn* _edgeTypes {nullptr};
};

class PipelineOutputInterface {
public:
    virtual PipelineInterfaceKind getKind() const = 0;

    PipelineOutputPort* getPort() const { return _port; }
    Dataframe* getDataframe() const { return _port->getBuffer()->getDataframe(); }

    void setPort(PipelineOutputPort* port) { _port = port; }

    virtual void connectTo(PipelineNodeInputInterface& input) = 0;
    virtual void connectTo(PipelineBlockInputInterface& input) = 0;
    virtual void connectTo(PipelineEdgeInputInterface& input) = 0;

protected:
    PipelineOutputPort* _port {nullptr};

    PipelineOutputInterface() = default;
    virtual ~PipelineOutputInterface() = default;
};

class PipelineBlockOutputInterface : public PipelineOutputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::BLOCK; }

    void connectTo(PipelineBlockInputInterface& input) override {
        _port->connectTo(input.getPort());
    }

    // Invalid connections, throws PipelineException
    void connectTo(PipelineNodeInputInterface& input) override;
    void connectTo(PipelineEdgeInputInterface& input) override;
};

class PipelineNodeOutputInterface : public PipelineOutputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::NODE; }

    void setNodeIDs(NamedColumn* nodeIDs) { _nodeIDs = nodeIDs; }
    void setIndices(NamedColumn* indices) { _indices = indices; }

    NamedColumn* getIndices() const { return _indices; }

    NamedColumn* getNodeIDs() const { return _nodeIDs; }

    void connectTo(PipelineNodeInputInterface& input) override {
        input.setNodeIDs(_nodeIDs);
        _port->connectTo(input.getPort());
    }

    void connectTo(PipelineBlockInputInterface& input) override {
        _port->connectTo(input.getPort());
    }

    // Invalid connection, throws PipelineException
    void connectTo(PipelineEdgeInputInterface& input) override;

private:
    NamedColumn* _indices {nullptr};
    NamedColumn* _nodeIDs {nullptr};
};

class PipelineEdgeOutputInterface : public PipelineOutputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::EDGE; }

    void setEdges(NamedColumn* edgeIDs,
                  NamedColumn* edgeTypes,
                  NamedColumn* neighbourNodes) {
        _edgeIDs = edgeIDs;
        _edgeTypes = edgeTypes;
        _neighbourNodes = neighbourNodes;
    }

    void setIndices(NamedColumn* indices) { _indices = indices; }

    NamedColumn* getIndices() const { return _indices; }

    NamedColumn* getEdgeIDs() const { return _edgeIDs; }
    NamedColumn* getOtherNodes() const { return _neighbourNodes; }
    NamedColumn* getEdgeTypes() const { return _edgeTypes; }

    void connectTo(PipelineEdgeInputInterface& input) override {
        input.setEdges(_edgeIDs, _neighbourNodes, _edgeTypes);
        _port->connectTo(input.getPort());
    }

    void connectTo(PipelineNodeInputInterface& input) override {
        input.setNodeIDs(_neighbourNodes);
        _port->connectTo(input.getPort());
    }

    void connectTo(PipelineBlockInputInterface& input) override {
        _port->connectTo(input.getPort());
    }

private:
    NamedColumn* _indices {nullptr};
    NamedColumn* _edgeIDs {nullptr};
    NamedColumn* _neighbourNodes {nullptr};
    NamedColumn* _edgeTypes {nullptr};
};

class PipelineValuesOutputInterface : public PipelineOutputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::VALUES; }

    void setIndices(NamedColumn* indices) { _indices = indices; }
    void setValues(NamedColumn* values) { _values = values; }

    NamedColumn* getIndices() const { return _indices; }
    NamedColumn* getValues() const { return _values; }

    void connectTo(PipelineBlockInputInterface& input) override {
        _port->connectTo(input.getPort());
    }

    // Invalid connections, throws PipelineException
    void connectTo(PipelineNodeInputInterface& input) override;
    void connectTo(PipelineEdgeInputInterface& input) override;

private:
    NamedColumn* _indices {nullptr};
    NamedColumn* _values {nullptr};
};

class PipelineValueOutputInterface : public PipelineOutputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::VALUE; }

    void setIndices(NamedColumn* indices) { _indices = indices; }
    void setValue(NamedColumn* value) { _value = value; }

    NamedColumn* getIndices() const { return _indices; }
    NamedColumn* getValue() const { return _value; }

    void connectTo(PipelineBlockInputInterface& input) override {
        _port->connectTo(input.getPort());
    }

    // Invalid connections, throws PipelineException
    void connectTo(PipelineNodeInputInterface& input) override;
    void connectTo(PipelineEdgeInputInterface& input) override;

private:
    NamedColumn* _indices {nullptr};
    NamedColumn* _value {nullptr};
};

}
