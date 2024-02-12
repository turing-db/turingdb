#pragma once

#include <string>

#include "StringSpan.h"

class Buffer;

namespace workflow {

class WorkflowServerHandler {
public:
    void process(Buffer* outBuffer,
                 const std::string& uri,
                 StringSpan payload);
};

}
