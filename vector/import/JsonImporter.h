#pragma once

#include <string>

#include "ImportResult.h"

namespace vec {

class BatchVectorCreate;

class JsonImporter {
public:
    static vec::ImportResult<void> import(BatchVectorCreate& batch,
                                          const std::string& data);
};

}
