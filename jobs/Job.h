#pragma once

#include <functional>
#include <memory>
#include <stdint.h>

#include "Promise.h"

namespace db {

/* @brief Closure to be executed by the JobSystem
 *
 * Takes in an abstract Promise* that can be casted
 * into a TypedPromise<T>
 * */
using JobOperation = std::function<void(Promise*)>;
using JobID = uint64_t;

struct Job {
    JobOperation _operation;
    std::unique_ptr<Promise> _promise;
};

}
