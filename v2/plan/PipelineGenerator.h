#pragma once

#include <memory>

namespace db::v2 {

class PlanGraph;
class PipelineV2;

class PipelineGenerator {
public:
    PipelineGenerator(const PlanGraph& graph);
    ~PipelineGenerator();

    std::unique_ptr<PipelineV2> generate();

private:
    const PlanGraph& _graph;
};

}
