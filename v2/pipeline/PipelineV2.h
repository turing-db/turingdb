#pragma once

#include <vector>
#include <memory>

namespace db {

class Processor;
class PipelineBuffer;

class PipelineV2 {
public:
    PipelineV2();
    ~PipelineV2();

private:
    std::vector<std::unique_ptr<Processor>> _processors;
    std::vector<std::unique_ptr<PipelineBuffer>> _buffers;
};

}
