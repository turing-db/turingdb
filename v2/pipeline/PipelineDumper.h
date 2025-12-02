#pragma once

#include <ostream>

namespace db::v2 {

class PipelineV2;

class PipelineDumper {
public:
    PipelineDumper(const PipelineV2* pipeline)
        : _pipeline(pipeline)
    {
    }

    void dumpMermaid(std::ostream& out);

private:
    const PipelineV2* _pipeline {nullptr};
};

}
