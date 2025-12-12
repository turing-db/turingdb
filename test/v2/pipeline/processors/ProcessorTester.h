#pragma once

#include "TuringTest.h"

#include "ExecutionContext.h"
#include "Graph.h"
#include "PipelineBuilder.h"

#include "PipelineExecutor.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "PipelineV2.h"

#include "TuringTestEnv.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;

#define EXECUTE(view, chunkSize)                          \
    {                                                     \
        ExecutionContext execCtxt(&_env->getSystemManager(), view); \
        execCtxt.setChunkSize(chunkSize);                 \
        _pipeline.clear();                                \
        PipelineExecutor executor(&_pipeline, &execCtxt); \
        executor.execute();                               \
    }

class ProcessorTester : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _builder = std::make_unique<PipelineBuilder>(&_env->getMem(), &_pipeline);
    }

    auto readGraph() {
        if (_graph == nullptr) {
            throw std::logic_error("Graph not initialized");
        }

        FrozenCommitTx transaction = _graph->openTransaction();
        GraphView view = transaction.viewGraph();
        GraphReader reader = transaction.readGraph();

        return std::make_tuple(transaction, view, reader);
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<PipelineBuilder> _builder;
    Graph* _graph {nullptr};

    PipelineV2 _pipeline;
};
