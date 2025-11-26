#pragma once

#include "QueryCallback.h"

#include "PipelineBuilder.h"
#include "views/GraphView.h"

namespace db {
class LocalMemory;
}

namespace db::v2 {

class SourceManager;
class ProcedureBlueprintMap;
class PlanGraph;
class PipelineV2;

class PipelineGenerator {
public:
    PipelineGenerator(LocalMemory* mem,
                      SourceManager* srcMan,
                      const PlanGraph* graph,
                      const GraphView& view,
                      PipelineV2* pipeline,
                      const QueryCallbackV2& callback);

    ~PipelineGenerator();

    void generate();

private:
    LocalMemory* _mem {nullptr};
    SourceManager* _sourceManager {nullptr};
    const PlanGraph* _graph {nullptr};
    const ProcedureBlueprintMap* _blueprints {nullptr};
    GraphView _view;
    PipelineV2* _pipeline {nullptr};
    QueryCallbackV2 _callback;
    PipelineBuilder _builder;
};

}
