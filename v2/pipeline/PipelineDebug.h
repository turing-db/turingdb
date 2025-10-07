#pragma once

#include <ostream>

namespace db::v2 {

class PipelineV2;

class PipelineDebug {
public:
    static void dumpMermaid(std::ostream& output, PipelineV2* pipeline);
};

}
