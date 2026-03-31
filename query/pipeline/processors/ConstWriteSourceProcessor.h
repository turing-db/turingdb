#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class Column;
class NamedColumn;

class ConstWriteSourceProcessor final : public Processor {
public:
    static ConstWriteSourceProcessor* create(PipelineV2* pipeline,
                                             Column* nodeIDs,
                                             Column* values);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineBlockOutputInterface& output() { return _output; }

    NamedColumn* nodeIDOutputCol() const { return _nodeIDOutputCol; }
    NamedColumn* valuesOutputCol() const { return _valuesOutputCol; }

private:
    friend class PipelineBuilder;

    PipelineBlockOutputInterface _output;

    Column* _nodeIDs;
    Column* _values;
    NamedColumn* _nodeIDOutputCol {nullptr};
    NamedColumn* _valuesOutputCol {nullptr};

    size_t _offset {0};

    ConstWriteSourceProcessor(Column* nodeIDs, Column* values);
    ~ConstWriteSourceProcessor() final;
};

}
