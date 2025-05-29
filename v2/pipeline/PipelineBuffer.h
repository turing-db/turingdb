#pragma once

namespace db {

class Processor;
class Column;

class PipelineBuffer {
public:
    PipelineBuffer()
    {
    }

    Processor* getInput() const { return _input; }
    Processor* getOutput() const { return _output; }
    Column* getData() const { return _data; }

private:
    Processor* _input {nullptr};
    Processor* _output {nullptr};
    Column* _data {nullptr};
};

}
