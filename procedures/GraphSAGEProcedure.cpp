#include "GraphSAGEProcedure.h"

#include "Procedure.h"
#include "ProcedureData.h"
#include "ProcedureNamespace.h"
#include "ProcedureState.h"

#include "FatalException.h"

using namespace db;

namespace {

void prepareImpl(ProcedureState* state) {
}

void executeImpl(ProcedureState* state) {
}

struct Data final : public ProcedureData {
};

}

ProcedureData* GraphSAGEProcedure::allocData() {
    return new Data();
}

void GraphSAGEProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void GraphSAGEProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("graphSAGE");

    proc->addArgument("seeds", ProcedureType::NODE);
    proc->addConstantArgument("fanouts", ProcedureType::LIST);
    proc->addOptionalConstantArgument("seed", ProcedureType::INT64);
    
    ns->addProcedure(proc);
}

void GraphSAGEProcedure::execute(ProcedureState* state) {
    switch (state->getStep()) {
        case ProcedureState::Step::PREPARE:
            prepareImpl(state);
            throw FatalException("prepare not implemented");
        break;

        case ProcedureState::Step::RESET:
            throw FatalException("reset not implemented");
        break;

        case ProcedureState::Step::EXECUTE:
            executeImpl(state);
            throw FatalException("execute not implemented");
        break;
    }
}
