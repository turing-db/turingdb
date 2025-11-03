#pragma once

namespace db::v2 {

class PipelineV2;
class PipelineOutputInterface;

class PipelineBuilder {
public:
    PipelineBuilder(PipelineV2* pipeline)
        : _pipeline(pipeline)
    {
    }

    PipelineBuilder& addScanNodes();
    PipelineBuilder& addGetOutEdges();

private:
    PipelineV2* _pipeline {nullptr};
    PipelineOutputInterface* _pendingOutput {nullptr};
};

}
