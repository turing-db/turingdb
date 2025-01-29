#pragma once

#include <vector>
#include <span>

#include "PipelineStep.h"

namespace db {

class Pipeline {
public:
    Pipeline();
    ~Pipeline();

    std::span<PipelineStep> steps() { return _steps; }

    template <typename StepT, typename... ArgsT>
    PipelineStep& add(ArgsT&&... args) {
        _steps.emplace_back(typename StepT::Tag {}, args...);
        return _steps.back();
    }

private:
    std::vector<PipelineStep> _steps;
};

}
