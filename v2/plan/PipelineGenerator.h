#pragma once

namespace db {
class LocalMemory;
}

namespace db::v2 {

class PlanGraph;
class PipelineV2;
class PlanGraphNode;
class PlanGraphStream;

class PipelineGenerator {
public:
    PipelineGenerator(const PlanGraph* graph,
                      PipelineV2* pipeline,
                      LocalMemory* mem)
        : _graph(graph),
        _pipeline(pipeline),
        _mem(mem)
    {
    }

    ~PipelineGenerator() = default;

    void generate();

private:
    const PlanGraph* _graph {nullptr};
    PipelineV2* _pipeline {nullptr};
    LocalMemory* _mem {nullptr};

    void translateNode(PlanGraphNode* node, PlanGraphStream& stream);
};

}
