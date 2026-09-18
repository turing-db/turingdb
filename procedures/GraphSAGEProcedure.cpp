#include "GraphSAGEProcedure.h"

#include <algorithm>
#include <array>
#include <memory>
#include <string>

#include <range/v3/view/join.hpp>

#include "samplers/GraphSAGESampler.h"

#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureNamespace.h"
#include "ProcedureState.h"

#include "ProcedureTypeVector.h"
#include "columns/ColumnConst.h"

#include "list/ListBufferTypeTag.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

namespace {

constexpr size_t returnValuesPerHop = 3;

using HopReturnValueNames = std::array<std::string, returnValuesPerHop>;

const auto returnValueNames = [] consteval {
    std::array<HopReturnValueNames, GraphSAGEProcedure::numHops> names;

    for (size_t hop {0}; auto& [dst, src, tgt] : names) {
        const char hopChar = hop + '0';
        dst = std::string {"dst_nodes"}, dst += hopChar;
        src = std::string {"src_nodes"}, src += hopChar;
        tgt = std::string {"tgt_nodes"}, tgt += hopChar;
        hop++;
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
        throw TuringException("graphSAGE() seeds must be a list of ints");
    }
}

void validateInput(Data& data) {
    {
        const Column* erased =  data.getInputColumn(0);
        const auto* seeds = dynamic_cast<const ColumnConst<ListView>*>(erased);
        bioassert(seeds, "Invalid seed column");
        numericList(seeds->getRaw());
    }
    {
        const Column* erased = data.getInputColumn(1);
        const auto* fanouts = dynamic_cast<const ColumnConst<ListView>*>(erased);
        bioassert(fanouts, "Invalid fanouts column");
        numericList(fanouts->getRaw());
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

    for (const std::string_view name : returnValueNames | rv::join) {
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
