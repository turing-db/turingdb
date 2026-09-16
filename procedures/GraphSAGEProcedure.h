#pragma once

#include "samplers/GraphSAGESampler.h"

#include <stddef.h>

namespace db {

class ProcedureData;
class ProcedureState;
class ProcedureNamespace;

struct GraphSAGEProcedure {
    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);

    static constexpr size_t numHops = GraphSAGESampler::hops;
};

}
