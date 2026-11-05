#include "GraphSAGEProcedure.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <spdlog/fmt/bundled/format.h>

#include "samplers/GraphSAGESampler.h"

#include "ProcUtils.h"
#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureException.h"
#include "ProcedureNamespace.h"
#include "TypedProcedure.h"
#include "ProcedureState.h"
#include "ProcedureTypeVector.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"

#include "CellTag.h"
#include "CellView.h"
#include "list/ListView.h"

#include "metadata/PropertyType.h"

#include "BioAssert.h"

using namespace db;

namespace {

constexpr std::string_view fanoutSizeErr =
    "Fanout parameter must be a list of size {}, not {}.";

constexpr std::string_view fanoutWidthErr =
    "Fanout {} exceeds the maximum of {}: a hop's sample must fit in one chunk.";

constexpr std::string_view NEGATIVE_FANOUT_ERROR = "Fanout {} is negative.";

constexpr std::string_view SEEDS_ERROR = "graphSAGE() seeds must be a list of nodes or ints";

constexpr std::string_view FANOUTS_ERROR = "graphSAGE() fanouts must be a list of ints";

constexpr std::string_view seedErr = "graphSAGE() seed must be an int";

constexpr size_t returnValuesPerHop = 3;

static_assert(GraphSAGEProcedure::numHops == 3, "Update the return values");

struct Data final : public TypedProcedureData<GraphSAGEProcedure, IndexedProcedureData> {
    std::unique_ptr<GraphSAGESampler> _sampler;
    ColumnNodeIDs _seeds;

    size_t _row {0};
    bool _started {false};
};

void readSeeds(const ListView list, ColumnNodeIDs& seeds) {
    for (const CellView element : list) {
        const CellTag tag = element.getTag();

        if (tag == CellTag::NodeID) {
            seeds.push_back(element.getAs<NodeID>());
        } else if (tag == CellTag::Int) {
            seeds.emplace_back(element.getAs<types::Int64::Primitive>());
        } else {
            throw ProcedureException(std::string(SEEDS_ERROR));
        }
    }
}

void readFanouts(const ListView list, size_t chunkSize, GraphSAGESampler::Fanouts& fanouts) {
    const size_t listSize = list.size();
    if (listSize != GraphSAGESampler::hops) {
        throw ProcedureException(fmt::format(fanoutSizeErr, GraphSAGESampler::hops, listSize));
    }

    size_t hop = 0;
    for (const CellView element : list) {
        if (element.getTag() != CellTag::Int) {
            throw ProcedureException(std::string(FANOUTS_ERROR));
        }

        const int64_t fanout = element.getAs<types::Int64::Primitive>();
        if (fanout < 0) {
            throw ProcedureException(fmt::format(NEGATIVE_FANOUT_ERROR, fanout));
        } else if (static_cast<size_t>(fanout) > chunkSize) {
            throw ProcedureException(fmt::format(fanoutWidthErr, fanout, chunkSize));
        }

        fanouts[hop] = fanout;
        hop++;
    }
}

// A null seed list names no node, so its row is sampled from no seeds
void startSample(Data& data, size_t chunkSize) {
    const size_t row = data._row;

    const std::optional<ListView> fanoutList =
        ProcUtils::rowArgument<ListView>(data.getInputColumn(1), row, FANOUTS_ERROR);
    if (!fanoutList) {
        throw ProcedureException(fmt::format(fanoutSizeErr, GraphSAGESampler::hops, "null"));
    }

    GraphSAGESampler::Fanouts fanouts {};
    readFanouts(*fanoutList, chunkSize, fanouts);

    const std::optional<ListView> seeds =
        ProcUtils::rowArgument<ListView>(data.getInputColumn(0), row, SEEDS_ERROR);

    data._seeds.clear();
    if (seeds) {
        readSeeds(*seeds, data._seeds);
    }

    const std::optional<uint64_t> rngSeed =
        ProcUtils::rowArgument<types::Int64::Primitive>(data.getInputColumn(2), row, seedErr);

    data._sampler->start(&data._seeds, fanouts, rngSeed);
    data._started = true;
}

template <size_t Hop>
void bindHopColumns(Data& data) {
    constexpr size_t base = Hop * returnValuesPerHop;

    data._sampler->setHopColumns(Hop,
                                 data.getReturnColumn<base + 1>(),
                                 data.getReturnColumn<base + 2>(),
                                 data.getReturnColumn<base>());
}

template <size_t... Hops>
void bindHopsColumns(Data& data, std::index_sequence<Hops...>) {
    (bindHopColumns<Hops>(data), ...);
}

void rewind(Data& data) {
    data._row = 0;
    data._started = false;
}

void prepareImpl(ProcedureState* state) {
    Data& data = state->data<Data>();

    const ProcedureContext* ctxt = state->getContext();
    const GraphView& view = *ctxt->getGraphView();

    data._sampler = std::make_unique<GraphSAGESampler>(view);
    bindHopsColumns(data, std::make_index_sequence<GraphSAGESampler::hops> {});

    rewind(data);
}

// A step packs the samples of consecutive rows into its budget, and ends early on a
// sample the budget cuts short, which the next step resumes
void executeImpl(ProcedureState* state) {
    Data& data = state->data<Data>();

    GraphSAGESampler* sampler = data._sampler.get();
    bioassert(sampler, "Null sampler");

    ColumnIndices* indices = data.indices();

    data.clearReturnColumns();
    if (indices) {
        indices->clear();
    }

    const size_t rowCount = data.getArgumentRowCount();
    const size_t chunkSize = state->getContext()->getChunkSize();
    size_t budget = chunkSize;

    while (data._row < rowCount && budget > 0) {
        if (!data._started) {
            startSample(data, chunkSize);
        }

        const size_t sampledRows = sampler->sample(budget);
        budget -= sampledRows;

        if (indices) {
            std::vector<size_t>& inputRows = indices->getRaw();
            inputRows.resize(inputRows.size() + sampledRows, data._row);
        }

        if (!sampler->finished()) {
            return;
        }

        data._row++;
        data._started = false;
    }

    if (data._row == rowCount) {
        state->finish();
    }
}

}

void GraphSAGEProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = createTypedProcedure<Data>("graphSAGE");

    proc->addArgument("seeds", ProcedureType::LIST);
    proc->addArgument("fanouts", ProcedureType::LIST);
    proc->addOptionalArgument("seed", ProcedureType::INT64);

    ns->addProcedure(proc);
}

void GraphSAGEProcedure::execute(ProcedureState* state) {
    switch (state->getStep()) {
        case ProcedureState::Step::PREPARE:
            prepareImpl(state);
        break;

        case ProcedureState::Step::RESET:
            rewind(state->data<Data>());
        break;

        case ProcedureState::Step::EXECUTE:
            executeImpl(state);
        break;
    }
}
