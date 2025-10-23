#pragma once

#include "NamedColumn.h"

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

    void setPort(PipelineInputPort* port) { _port = port; }

private:
    PipelineInputPort* _port {nullptr};
};

class PipelineOutputInterface  : public PipelineInterface {
public:
    PipelineOutputPort* getPort() const { return _port; }

    void setPort(PipelineOutputPort* port) { _port = port; }

private:
    PipelineOutputPort* _port {nullptr};
};

}
