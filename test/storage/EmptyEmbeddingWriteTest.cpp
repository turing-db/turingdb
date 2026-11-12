#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"
#include "metadata/PropertyTypeMap.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

#include "FatalException.h"
#include "TuringException.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

const std::vector<float> NONE;
const std::vector<float> TWO {1.0f, 0.0f};

constexpr std::string_view EMPTY_VEC = "Embedding property 'vec' cannot take an empty embedding";

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

class EmptyEmbeddingWriteTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void expectDimension(const Graph* graph, std::optional<size_t> expected) {
        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphView view = transaction.viewGraph();
        const PropertyTypeMap& propertyTypes = view.metadata().propTypes();

        const std::optional<PropertyType> vec = propertyTypes.get("vec");
        ASSERT_TRUE(vec.has_value());
        EXPECT_EQ(propertyTypes.getEmbeddingDimension(vec->_id), expected);
    }

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(EmptyEmbeddingWriteTest, refusesAnEmptyNodeEmbeddingBeforeItFixesTheDimension) {
    auto graph = Graph::create();
    GraphWriter writer(graph.get(), _jobSystem.get());

    const NodeID empty = writer.addNode({"Vec"});
    expectUserError([&] { writer.addNodeProperty<types::Embedding>(empty, "vec", std::span<const float> {NONE}); },
                    EMPTY_VEC);

    const NodeID filled = writer.addNode({"Vec"});
    writer.addNodeProperty<types::Embedding>(filled, "vec", std::span<const float> {TWO});
    ASSERT_TRUE(writer.submit());

    expectDimension(graph.get(), 2);
}

TEST_F(EmptyEmbeddingWriteTest, refusesAnEmptyEdgeEmbeddingBeforeItFixesTheDimension) {
    auto graph = Graph::create();
    GraphWriter writer(graph.get(), _jobSystem.get());

    const NodeID source = writer.addNode({"Vec"});
    const NodeID target = writer.addNode({"Vec"});
    const EdgeRecord edge = writer.addEdge("LINK", source, target);
    expectUserError([&] { writer.addEdgeProperty<types::Embedding>(edge, "vec", std::span<const float> {NONE}); },
                    EMPTY_VEC);

    writer.addEdgeProperty<types::Embedding>(edge, "vec", std::span<const float> {TWO});
    ASSERT_TRUE(writer.submit());

    expectDimension(graph.get(), 2);
}

TEST_F(EmptyEmbeddingWriteTest, refusesAnEmptyEmbeddingOfAPropertyThatHasADimension) {
    auto graph = Graph::create();
    GraphWriter writer(graph.get(), _jobSystem.get());

    const NodeID filled = writer.addNode({"Vec"});
    writer.addNodeProperty<types::Embedding>(filled, "vec", std::span<const float> {TWO});

    const NodeID empty = writer.addNode({"Vec"});
    expectUserError([&] { writer.addNodeProperty<types::Embedding>(empty, "vec", std::span<const float> {NONE}); },
                    EMPTY_VEC);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
