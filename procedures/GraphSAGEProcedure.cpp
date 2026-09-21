#include "GraphSAGEProcedure.h"

#include <algorithm>
#include <array>
#include <memory>
#include <string>
#include <string_view>

#include <range/v3/view/join.hpp>

#include <spdlog/fmt/bundled/format.h>

#include "BioAssert.h"
#include "iterators/ChunkConfig.h"
#include "samplers/GraphSAGESampler.h"

#include "ProcUtils.h"
#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureNamespace.h"
#include "ProcedureState.h"
#include "ProcedureTypeVector.h"

#include "columns/Column.h"
#include "columns/ColumnConst.h"

#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"

#include "metadata/PropertyType.h"

#include "TuringException.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

namespace {

constexpr std::string_view fanoutSizeErr =
    "Fanout parameter must be a list of size {}, not {}.";

constexpr std::string_view fanoutWidthErr =
    "Fanout {} exceeds the maximum of {}: a hop's sample must fit in one chunk.";

constexpr std::string_view seedErr = "graphSAGE() seed must be a constant int";

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

void numericList(const ListView l) {
    const auto isInt = [](ListElementView ele) -> bool {
        return ele.getTag() == ListBufferTypeTag::Int;
    };
    const bool allInts = std::ranges::all_of(l, isInt);
    if (!allInts) {
        throw TuringException("graphSAGE() seeds must be a list of ints");
    }
}

void validFanoutList(const ListView l) {
    const size_t listSize = l.size();
    constexpr size_t reqSize = GraphSAGESampler::hops;

    if (listSize != reqSize) {
        throw TuringException(fmt::format(fanoutSizeErr, reqSize, listSize));
    }

    // One node's sample is emitted whole, so a fanout wider than a chunk could not be
    // returned without a step running over the row budget it promises
    for (const ListElementView ele : l) {
        const size_t fanout = ele.getAs<types::Int64::Primitive>();
        if (fanout > ChunkConfig::CHUNK_SIZE) {
            throw TuringException(fmt::format(fanoutWidthErr, fanout, ChunkConfig::CHUNK_SIZE));
        }
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
        validFanoutList(fanouts->getRaw());
    }
}

GraphSAGESampler::NodeCol* nodeColumn(Data& data, size_t index) {
    Column* col = data.getReturnColumn(index);
    if (!col) {
        return nullptr;
    }

    return col->cast<GraphSAGESampler::NodeCol>();
}

void prepareImpl(ProcedureState* state) {
    Data& data = state->data<Data>();
    validateInput(data);

    const size_t seed = [&] -> size_t {
        const Column* col = data.getInputColumn(2);

        return col ? ProcUtils::constArg<types::Int64::Primitive>(col, seedErr)
                   : GraphSAGESampler::NOSEED;
    }();

    const ProcedureContext* ctxt = state->getContext();
    const GraphView& view = *ctxt->getGraphView();

    const GraphSAGESampler::Fanouts fanouts = [&] -> auto {
        GraphSAGESampler::Fanouts out;
        using FanoutColType = const ColumnConst<ListView>;
        const Column* col = data.getInputColumn(1);
        const auto* fanoutsCol = dynamic_cast<FanoutColType*>(col);
        bioassert(fanoutsCol, "Invalid fanouts col passed validation");
        const ListView l = fanoutsCol->getRaw();
        const auto eles = l.elements();
        for (size_t i = 0; i < GraphSAGESampler::hops; i++) {
            const size_t fanout = eles[i].getAs<types::Int64::Primitive>();
            out[i] = fanout;
        }
        return out;
    }();

    data.sampler = std::make_unique<GraphSAGESampler>(view, seed);

    for (size_t hop = 0; hop < GraphSAGESampler::hops; hop++) {
        const size_t base = hop * returnValuesPerHop;

        GraphSAGESampler::NodeCol* dst = nodeColumn(data, base);
        GraphSAGESampler::NodeCol* srcs = nodeColumn(data, base + 1);
        GraphSAGESampler::NodeCol* tgts = nodeColumn(data, base + 2);

        data.sampler->setHopData(hop, srcs, tgts, dst, fanouts[hop]);
    }

    ColumnNodeIDs nodes;
    const Column* x = data.getInputColumn(0);
    const auto* seeds = dynamic_cast<const ColumnConst<ListView>*>(x);
    bioassert(seeds, "invalid seeds");
    const ListView list = seeds->getRaw();
    for (const ListElementView ele : list) {
        nodes.emplace_back(ele.getAs<types::Int64::Primitive>());
    }

    data.sampler->seed(&nodes);
}

void executeImpl(ProcedureState* state) {
    Data& data = state->data<Data>();

    std::unique_ptr<GraphSAGESampler>& sampler = data.sampler;

    bioassert(sampler, "Null sampler");

    const size_t chunkSize = state->getContext()->getChunkSize();
    sampler->sample(chunkSize);

    if (sampler->finished()) {
        state->finish();
    }
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
        proc->addNullableReturnValue(name, ProcedureType::NODE);
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

        case ProcedureState::Step::EXECUTE: {
            executeImpl(state);
        }
        break;
    }
}
