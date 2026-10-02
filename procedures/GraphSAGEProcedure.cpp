#include "GraphSAGEProcedure.h"

#include <algorithm>
#include <array>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <spdlog/fmt/bundled/format.h>

#include "samplers/GraphSAGESampler.h"

#include "ProcUtils.h"
#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureNamespace.h"
#include "TypedProcedure.h"
#include "ProcedureState.h"
#include "ProcedureTypeVector.h"

#include "columns/Column.h"
#include "columns/ColumnConst.h"

#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"

#include "metadata/PropertyType.h"

#include "iterators/ChunkConfig.h"

#include "TuringException.h"
#include "BioAssert.h"

using namespace db;

namespace {

constexpr std::string_view fanoutSizeErr =
    "Fanout parameter must be a list of size {}, not {}.";

constexpr std::string_view fanoutWidthErr =
    "Fanout {} exceeds the maximum of {}: a hop's sample must fit in one chunk.";

constexpr std::string_view seedErr = "graphSAGE() seed must be a constant int";

constexpr size_t returnValuesPerHop = 3;

static_assert(GraphSAGEProcedure::numHops == 3, "Update the return values");

struct Data final : public ProcedureData {
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

    for (const ListElementView ele : l) {
        const size_t fanout = ele.getAs<types::Int64::Primitive>();
        if (fanout > ChunkConfig::CHUNK_SIZE) {
            throw TuringException(fmt::format(fanoutWidthErr, fanout, ChunkConfig::CHUNK_SIZE));
        }
    }
}

void validateInput(Data& data) {
    {
        const Column* seedsCol =  data.getInputColumn(0);
        const ListView seeds = ProcUtils::constArg<ListView>(seedsCol, "Invalid seed column.");
        numericList(seeds);
    }
    {
        const Column* fanoutsCol = data.getInputColumn(1);
        const ListView fanouts = ProcUtils::constArg<ListView>(fanoutsCol, "Invalid fanouts column");
        numericList(fanouts);
        validFanoutList(fanouts);
    }
}

template <size_t Hop>
void setHopData(Data& data, const GraphSAGESampler::Fanouts& fanouts) {
    constexpr size_t base = Hop * returnValuesPerHop;

    data.sampler->setHopData(Hop,
                             getReturnColumn<GraphSAGEProcedure, base + 1>(&data),
                             getReturnColumn<GraphSAGEProcedure, base + 2>(&data),
                             getReturnColumn<GraphSAGEProcedure, base>(&data),
                             fanouts[Hop]);
}

template <size_t... Hops>
void setHopsData(Data& data, const GraphSAGESampler::Fanouts& fanouts, std::index_sequence<Hops...>) {
    (setHopData<Hops>(data, fanouts), ...);
}

// The seeds are a constant argument, so they are read the same way whether the call is
// being prepared or rewound for another drive
void seedSampler(Data& data) {
    ColumnNodeIDs nodes;

    const Column* seedsErased = data.getInputColumn(0);
    const auto* seeds = dynamic_cast<const ColumnConst<ListView>*>(seedsErased);
    bioassert(seeds, "Invalid seeds");

    const ListView list = seeds->getRaw();
    for (const ListElementView ele : list) {
        nodes.emplace_back(ele.getAs<types::Int64::Primitive>());
    }

    data.sampler->seed(&nodes);
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

    setHopsData(data, fanouts, std::make_index_sequence<GraphSAGESampler::hops> {});

    seedSampler(data);
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

void GraphSAGEProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = createTypedProcedure<GraphSAGEProcedure, Data>("graphSAGE");

    proc->addConstantArgument("seeds", ProcedureType::LIST);
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
            Data& data = state->data<Data>();
            if (data.sampler) {
                data.sampler->reset();
                seedSampler(data);
            }
        }
        break;

        case ProcedureState::Step::EXECUTE: {
            executeImpl(state);
        }
        break;
    }
}
