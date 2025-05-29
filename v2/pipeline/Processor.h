#pragma once

#include <vector>

namespace db {

class PipelineBuffer;

class Processor {
public:
    Processor();
    virtual ~Processor();

    virtual void execute() = 0;

private:
    std::vector<PipelineBuffer*> _inputs;
    std::vector<PipelineBuffer*> _outputs;
};

}
