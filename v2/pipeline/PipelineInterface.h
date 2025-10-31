#pragma once

#include "PipelinePort.h"

#include "dataframe/NamedColumn.h"

namespace db::v2 {

class PipelineInputPort;
class PipelineOutputPort;

class PipelineInterface {
public:
    NamedColumn* getColumn() const { return _col; }
    Column* getRawColumn() const { return _col->getColumn(); }

    void setColumn(NamedColumn* col) { _col = col; }

protected:
    PipelineInterface() = default;

private:
    NamedColumn* _col {nullptr};
};

class PipelineInputInterface : public PipelineInterface {
public:
    PipelineInputPort* getPort() const { return _port; }
    Dataframe* getDataframe() const { return _port->getBuffer()->getDataframe(); }

    void setPort(PipelineInputPort* port) { _port = port; }

private:
    PipelineInputPort* _port {nullptr};
};

class PipelineOutputInterface  : public PipelineInterface {
public:
    PipelineOutputPort* getPort() const { return _port; }
    Dataframe* getDataframe() const { return _port->getBuffer()->getDataframe(); }

    void setPort(PipelineOutputPort* port) { _port = port; }

    void connectTo(PipelineInputInterface& input) {
        _port->connectTo(input.getPort());
        input.setColumn(getColumn());
    }

private:
    PipelineOutputPort* _port {nullptr};
};

}
