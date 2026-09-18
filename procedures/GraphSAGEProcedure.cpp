#include "GraphSAGEProcedure.h"

#include <algorithm>
#include <array>
#include <memory>
#include <string>

#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureNamespace.h"
#include "ProcedureState.h"

#include "ProcedureTypeVector.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOperatorDispatcher.h"

#include "list/ListBufferTypeTag.h"

#include "FatalException.h"
#include "TuringException.h"
#include "samplers/GraphSAGESampler.h"

using namespace db;

namespace {

constexpr size_t returnValuesPerHop = 3;
constexpr size_t returnValueCount = GraphSAGEProcedure::numHops * returnValuesPerHop;

const auto returnValueNames = [] {
    std::array<std::string, returnValueCount> names;

    for (size_t hop = 0; hop < GraphSAGEProcedure::numHops; hop++) {
        const size_t base = hop * returnValuesPerHop;

        names[base] = fmt::format("dst_nodes{}", hop);
        names[base + 1] = fmt::format("src_nodes{}", hop);
        names[base + 2] = fmt::format("tgt_nodes{}", hop);
    }

    return names;
}();

struct Data final : public IndexedProcedureData {
    std::unique_ptr<GraphSAGESampler> sampler;
};

void numericList(ListView l) {
    const auto isInt = [](ListElementView ele) -> bool {
        return ele.getTag() == ListBufferTypeTag::Int;
    };
    const bool allInts = std::ranges::all_of(l, isInt);
    if (!allInts) {
        throw FatalException("graphSAGE() seeds must be a list of ints");
    }
}

void validateInput(Data& data) {
    const auto* seeds = [&data] -> const ColumnConst<ListView>* {
        const Column* erased =  data.getInputColumn(0);
        const auto* out = dynamic_cast<const ColumnConst<ListView>*>(erased);
        bioassert(out, "Invalid seed column");
        return out;
    }();

    numericList(seeds->getRaw());

    const auto* fanouts = [&data] -> const ColumnConst<ListView>* {
        const Column* erased = data.getInputColumn(1);
        const auto* out = dynamic_cast<const ColumnConst<ListView>*>(erased);
        bioassert(out, "Invalid fanouts column");
        return out;
    }();

    numericList(fanouts->getRaw());
}

void prepareImpl(ProcedureState* state) {
    Data& data = state->data<Data>();
    validateInput(data);

    const ProcedureContext* ctxt = state->getContext();
    const GraphView& view = *ctxt->getGraphView();

    data.sampler = std::make_unique<GraphSAGESampler>(view);
}

void executeImpl(ProcedureState* state) {
    /*
    Data& data = state->data<Data>();
    const Column* seeds = data.getInputColumn(0);
    */
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

    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);

    proc->addConstantArgument("seeds", ProcedureType::LIST);
    proc->addConstantArgument("fanouts", ProcedureType::LIST);
    proc->addOptionalConstantArgument("seed", ProcedureType::INT64);

    for (const std::string& name : returnValueNames) {
        proc->addReturnValue(name, ProcedureType::NODE);
    }

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
            state->finish();
            // throw FatalException("execute not implemented");
        break;
    }
}
