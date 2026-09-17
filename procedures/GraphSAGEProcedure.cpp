#include "GraphSAGEProcedure.h"

#include <algorithm>
#include <memory>

#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureNamespace.h"
#include "ProcedureState.h"

#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"

#include "list/ListBufferTypeTag.h"

#include "FatalException.h"
#include "TuringException.h"
#include "samplers/GraphSAGESampler.h"

using namespace db;

namespace {

struct Data final : public IndexedProcedureData {
    std::unique_ptr<GraphSAGESampler> sampler;
};

struct ListValidate {
    template <typename T>
    void operator()(ColumnVector<T>* /*unused*/) {
        throw FatalException("Instantiated without list");
    }
    template <typename T>
    void operator()(ColumnConst<T>* /*unused*/) {
        throw FatalException("Instantiated without list");
    }


    template <>
    void operator()(ColumnVector<ListView>* col) {
        if (col->size() != 1) {
            throw TuringException("graphSAGE() seeds must be a single list");
        }
        ListView list = col->front();
        numericList(list);
    }

    template <>
    void operator()(ColumnConst<ListView>* col) {
        ListView list = col->getRaw();
        numericList(list);
    }

    static void numericList(ListView l) {
        const auto isInt = [](ListElementView ele) -> bool {
            return ele.getTag() == ListBufferTypeTag::Int;
        };
        const bool allInts = std::ranges::all_of(l, isInt);
        if (!allInts) {
            throw FatalException("graphSAGE() seeds must be a list of ints");
        }
    }
};

void validateInput(Data& data) {
    { // ensure seeds are a singular list of integers
        const Column* seeds = data.getInputColumn(0);
        using Types = GraphSAGEInputs;
        using Dispatcher =
            ColumnSingleDispatcher<Types::Allowed, ListValidate, Types::Excluded>;

        ListValidate validator;
        Dispatcher::dispatch(seeds, validator);
    }
}

void prepareImpl(ProcedureState* state) {
    Data& data = state->data<Data>();
    validateInput(data);

    const ProcedureContext* ctxt = state->getContext();
    const GraphView& view = *ctxt->getGraphView();

    data.sampler = std::make_unique<GraphSAGESampler>(view);
}

void executeImpl(ProcedureState* state) {
    Data& data = state->data<Data>();
    const Column* seeds = data.getInputColumn(0);
}
}

ProcedureData* GraphSAGEProcedure::allocData() {
    return new Data();
}

void GraphSAGEProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void GraphSAGEProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("graphSAGE");

    proc->addArgument("seeds", ProcedureType::LIST);
    proc->addConstantArgument("fanouts", ProcedureType::LIST);
    proc->addOptionalConstantArgument("seed", ProcedureType::INT64);
    
    ns->addProcedure(proc);
}

void GraphSAGEProcedure::execute(ProcedureState* state) {
    switch (state->getStep()) {
        case ProcedureState::Step::PREPARE:
            prepareImpl(state);
        break;

        case ProcedureState::Step::RESET: {
            auto& sampler = state->data<Data>().sampler;
            if (sampler) {
                sampler->reset();
            }
        }
        break;

        case ProcedureState::Step::EXECUTE:
            executeImpl(state);
            throw FatalException("execute not implemented");
        break;
    }
}
