#include <gtest/gtest.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "datapart/DataPart.h"
#include "mergers/DataPartMerger.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "writers/DataPartBuilder.h"
#include "writers/GraphWriter.h"

#include "FatalException.h"
#include "TuringException.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

const std::vector<float> TWO {1.0f, 0.0f};
const std::vector<float> THREE {1.0f, 0.0f, 0.0f};

constexpr std::string_view TWO_THEN_THREE = "Embedding property 'vec' has dimension 2 and cannot take an embedding of dimension 3";

template <typename Write>
void expectUserError(Write write, std::string_view reason) {
    try {
        write();
        FAIL() << "accepted, expected: " << reason;
    } catch (const FatalException& e) {
        FAIL() << "internal error: " << e.what();
    } catch (const TuringException& e) {
        EXPECT_NE(std::string_view {e.what()}.find(reason), std::string_view::npos) << e.what();
    }
}

}

class EmbeddingPropertyDimensionTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void writeVecNode(Graph* graph, const std::vector<float>& embedding) {
        GraphWriter writer(graph, _jobSystem.get());

        const NodeID node = writer.addNode({"Vec"});
        writer.addNodeProperty<types::Embedding>(node, "vec", std::span<const float> {embedding});

        ASSERT_TRUE(writer.submit());
    }

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(EmbeddingPropertyDimensionTest, rejectsAnotherDimensionInOneDataPart) {
    auto graph = Graph::create();
    GraphWriter writer(graph.get(), _jobSystem.get());

    const NodeID first = writer.addNode({"Vec"});
    writer.addNodeProperty<types::Embedding>(first, "vec", std::span<const float> {TWO});

    const NodeID second = writer.addNode({"Vec"});
    expectUserError([&] { writer.addNodeProperty<types::Embedding>(second, "vec", std::span<const float> {THREE}); },
                    TWO_THEN_THREE);
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsAnotherDimensionThanAnEarlierCommitWrote) {
    auto graph = Graph::create();
    writeVecNode(graph.get(), TWO);

    GraphWriter writer(graph.get(), _jobSystem.get());
    const NodeID node = writer.addNode({"Vec"});

    expectUserError([&] { writer.addNodeProperty<types::Embedding>(node, "vec", std::span<const float> {THREE}); },
                    TWO_THEN_THREE);
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsAnEdgeEmbeddingOfAnotherDimensionThanTheNodes) {
    auto graph = Graph::create();
    writeVecNode(graph.get(), TWO);

    GraphWriter writer(graph.get(), _jobSystem.get());
    const NodeID source = writer.addNode({"Vec"});
    const NodeID target = writer.addNode({"Vec"});
    const EdgeRecord edge = writer.addEdge("LINK", source, target);

    expectUserError([&] { writer.addEdgeProperty<types::Embedding>(edge, "vec", std::span<const float> {THREE}); },
                    TWO_THEN_THREE);
}

TEST_F(EmbeddingPropertyDimensionTest, rejectsAnEdgeEmbeddingOfAnotherDimensionInOneDataPart) {
    auto graph = Graph::create();
    GraphWriter writer(graph.get(), _jobSystem.get());

    const NodeID source = writer.addNode({"Vec"});
    const NodeID target = writer.addNode({"Vec"});
    writer.addNodeProperty<types::Embedding>(source, "vec", std::span<const float> {TWO});

    const EdgeRecord edge = writer.addEdge("LINK", source, target);
    expectUserError([&] { writer.addEdgeProperty<types::Embedding>(edge, "vec", std::span<const float> {THREE}); },
                    TWO_THEN_THREE);
}

// The write path cannot build data parts that disagree, so the two parts come from two
// graphs, each of which accepted its own dimension. They stand for a graph written before
// the dimension was enforced, whose parts the merge must refuse rather than crash on.
TEST_F(EmbeddingPropertyDimensionTest, refusesToMergeDataPartsOfDifferentDimensions) {
    auto twoDimensional = Graph::create();
    writeVecNode(twoDimensional.get(), TWO);

    auto threeDimensional = Graph::create();
    writeVecNode(threeDimensional.get(), THREE);

    const FrozenCommitTx twoTransaction = twoDimensional->openTransaction();
    const FrozenCommitTx threeTransaction = threeDimensional->openTransaction();
    const std::vector<WeakArc<DataPart>> parts {twoTransaction.viewGraph().dataparts().front(),
                                                threeTransaction.viewGraph().dataparts().front()};

    const std::unique_ptr<Change> change = twoDimensional->newChange();
    CommitBuilder* tip = change->access().getTip();
    const DataPartMerger merger(&tip->commitData(), tip->metadata());

    expectUserError([&] { merger.merge(parts); },
                    "Cannot merge the data parts: embedding property 'vec' has dimension 2 and a data part holds embeddings of dimension 3");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
