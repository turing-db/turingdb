#pragma once

#include "Processor.h"

#include "PipelineInterface.h"

namespace db::v2 {

class PipelineV2;

class CartesianProductProcessor : public Processor {
public:
    static CartesianProductProcessor* create(PipelineV2* pipeline);
private:
    CartesianProductProcessor();
    ~CartesianProductProcessor() override;
};
  
}
