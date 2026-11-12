#include "TuringTest.h"

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "Path.h"
#include "FilePageWriter.h"
#include "comparators/GraphComparator.h"
#include "dump/DumpConfig.h"
#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"
#include "metadata/PropertyTypeMap.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/GraphWriter.h"
#include "writers/MetadataBuilder.h"

#include "FatalException.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

namespace {

const std::vector<float> TWO {1.0f, 0.0f};
const std::vector<float> THREE {1.0f, 0.0f, 0.0f};

constexpr std::string_view VEC_TWO_THEN_THREE = "Embedding property 'vec' has dimension 2 and cannot take an embedding of dimension 3";
constexpr std::string_view LINK_THREE_THEN_TWO = "Embedding property 'link' has dimension 3 and cannot take an embedding of dimension 2";
constexpr std::string_view MERGE_REFUSED = "Cannot merge the data parts: embedding property 'vec' has dimension 2 and a data part holds embeddings of dimension 3";

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

// The property types of a commit as a dumper older than the embedding dimension wrote them:
// each value type, then the name, and no dimension
void writePropertyTypesWithoutDimensions(const fs::Path& path, const PropertyTypeMap& propertyTypes) {
    auto opened = fs::FilePageWriter::open(path, DumpConfig::PAGE_SIZE);
    ASSERT_TRUE(opened);
    fs::FilePageWriter& writer = opened.value();

    const uint64_t propertyTypeCount = propertyTypes.getCount();
    const uint64_t pageCount = 1;

    writer.writeToCurrentPage(DumpConfig::ONE_BAD_CAFE);
    writer.writeToCurrentPage(DumpConfig::UP_TO_DATE_VERSION);
    writer.writeToCurrentPage(propertyTypeCount);
    writer.writeToCurrentPage(pageCount);
    writer.nextPage();

    writer.writeToCurrentPage(propertyTypeCount);
    for (const PropertyTypeMap::Pair& pair : propertyTypes) {
        const std::string& name = *pair._name;
        const uint64_t nameSize = name.size();

        writer.writeToCurrentPage(pair._pt._valueType);
        writer.writeToCurrentPage(nameSize);
        writer.writeToCurrentPage(name);
    }

    writer.finish();
    ASSERT_FALSE(writer.errorOccured());
}

}

class EmbeddingDimensionDumpLoadTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void dumpGraph(const Graph* graph, const fs::Path& path) {
        const auto dumped = GraphDumper::dump(graph, path);
        ASSERT_TRUE(dumped) << dumped.error().fmtMessage();
    }

    void loadGraph(Graph* graph, const fs::Path& path) {
        const auto loaded = GraphLoader::load(graph, path);
        ASSERT_TRUE(loaded) << loaded.error().fmtMessage();
    }

    void mergeDataParts(Graph* graph) {
        const auto merged = graph->mergeDataParts(*_jobSystem);
        ASSERT_TRUE(merged);
    }

    void dropDimensionsFromDump(const Graph* graph, const fs::Path& path) {
        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphView view = transaction.viewGraph();
        const GraphMetadata& metadata = view.metadata();

        const fs::Path commitDir = path / "commits" / std::to_string(graph->getHeadHash().get());
        writePropertyTypesWithoutDimensions(commitDir / "property-types", metadata.propTypes());
    }

    void expectNodeEmbeddingRejected(Graph* graph, const std::vector<float>& embedding, std::string_view reason) {
        GraphWriter writer(graph, _jobSystem.get());
        const NodeID node = writer.addNode({"Vec"});

        expectUserError([&] { writer.addNodeProperty<types::Embedding>(node, "vec", std::span<const float> {embedding}); },
                        reason);
    }

    void expectEdgeEmbeddingRejected(Graph* graph, const std::vector<float>& embedding, std::string_view reason) {
        GraphWriter writer(graph, _jobSystem.get());
        const NodeID source = writer.addNode({"Vec"});
        const NodeID target = writer.addNode({"Vec"});
        const EdgeRecord edge = writer.addEdge("LINKS", source, target);

        expectUserError([&] { writer.addEdgeProperty<types::Embedding>(edge, "link", std::span<const float> {embedding}); },
                        reason);
    }

    // Two data parts that disagree on the dimension of 'vec'. The second is built through a
    // metadata that has never seen a value of 'vec', as every builder was before the
    // dimension was enforced.
    void writeDisagreeingDataParts(Graph* graph) {
        NodeID second;
        {
            GraphWriter writer(graph, _jobSystem.get());
            const NodeID first = writer.addNode({"Vec"});
            second = writer.addNode({"Vec"});
            writer.addNodeProperty<types::Embedding>(first, "vec", std::span<const float> {TWO});
            ASSERT_TRUE(writer.submit());
        }

        const std::unique_ptr<Change> change = graph->newChange();
        ChangeAccessor access = change->access();
        CommitBuilder* tip = access.getTip();
        const GraphView view = tip->viewGraph();

        GraphMetadata unaware;
        const std::unique_ptr<MetadataBuilder> unawareBuilder = MetadataBuilder::create(unaware, &unaware);
        const PropertyType vec = unawareBuilder->getOrCreatePropertyType("vec", ValueType::Embedding);

        const GraphMetadata& metadata = view.metadata();
        const PropertyTypeMap& propertyTypes = metadata.propTypes();
        ASSERT_EQ(propertyTypes.get("vec"), vec);

        std::unique_ptr<DataPartBuilder> builder = DataPartBuilder::prepare(*unawareBuilder, view, view.dataparts().size());
        builder->addNodeProperty<types::Embedding>(second, vec._id, std::span<const float> {THREE});
        tip->appendBuilder(std::move(builder));

        ASSERT_TRUE(access.submit(*_jobSystem));
    }

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(EmbeddingDimensionDumpLoadTest, keepsTheDimensionOfAPropertyNoValueHoldsAnyMore) {
    auto graph = Graph::create("vectors", fs::Path {_outDir} / "original");
    {
        GraphWriter writer(graph.get(), _jobSystem.get());
        const NodeID source = writer.addNode({"Vec"});
        const NodeID target = writer.addNode({"Vec"});
        writer.addNodeProperty<types::Embedding>(source, "vec", std::span<const float> {TWO});

        const EdgeRecord edge = writer.addEdge("LINKS", source, target);
        writer.addEdgeProperty<types::Embedding>(edge, "link", std::span<const float> {THREE});
        ASSERT_TRUE(writer.submit());

        writer.deleteNode(source);
        ASSERT_TRUE(writer.submit());
    }
    mergeDataParts(graph.get());

    const fs::Path dumpPath = fs::Path {_outDir} / "dump";
    dumpGraph(graph.get(), dumpPath);

    auto loaded = Graph::create();
    loadGraph(loaded.get(), dumpPath);
    EXPECT_TRUE(GraphComparator::same(*graph, *loaded));

    expectNodeEmbeddingRejected(loaded.get(), THREE, VEC_TWO_THEN_THREE);
    expectEdgeEmbeddingRejected(loaded.get(), TWO, LINK_THREE_THEN_TWO);
}

TEST_F(EmbeddingDimensionDumpLoadTest, takesTheDimensionOfAnOlderDumpFromItsDataParts) {
    auto graph = Graph::create("vectors", fs::Path {_outDir} / "original");
    {
        GraphWriter writer(graph.get(), _jobSystem.get());
        const NodeID source = writer.addNode({"Vec"});
        const NodeID target = writer.addNode({"Vec"});
        writer.addNodeProperty<types::Embedding>(source, "vec", std::span<const float> {TWO});

        const EdgeRecord edge = writer.addEdge("LINKS", source, target);
        writer.addEdgeProperty<types::Embedding>(edge, "link", std::span<const float> {THREE});
        ASSERT_TRUE(writer.submit());

        writer.deleteNode(source);
        ASSERT_TRUE(writer.submit());
    }

    const fs::Path dumpPath = fs::Path {_outDir} / "dump";
    dumpGraph(graph.get(), dumpPath);
    dropDimensionsFromDump(graph.get(), dumpPath);

    auto loaded = Graph::create();
    loadGraph(loaded.get(), dumpPath);
    EXPECT_TRUE(GraphComparator::same(*graph, *loaded));

    mergeDataParts(loaded.get());
    expectNodeEmbeddingRejected(loaded.get(), THREE, VEC_TWO_THEN_THREE);
    expectEdgeEmbeddingRejected(loaded.get(), TWO, LINK_THREE_THEN_TWO);
}

TEST_F(EmbeddingDimensionDumpLoadTest, loadsDataPartsThatDisagreeAndRefusesToMergeThem) {
    auto graph = Graph::create("vectors", fs::Path {_outDir} / "original");
    writeDisagreeingDataParts(graph.get());

    const fs::Path dumpPath = fs::Path {_outDir} / "dump";
    dumpGraph(graph.get(), dumpPath);

    const fs::Path olderDumpPath = fs::Path {_outDir} / "older-dump";
    dumpGraph(graph.get(), olderDumpPath);
    dropDimensionsFromDump(graph.get(), olderDumpPath);

    for (const fs::Path& path : {dumpPath, olderDumpPath}) {
        auto loaded = Graph::create();
        loadGraph(loaded.get(), path);

        expectNodeEmbeddingRejected(loaded.get(), THREE, VEC_TWO_THEN_THREE);
        expectUserError([&] { mergeDataParts(loaded.get()); }, MERGE_REFUSED);
    }
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv);
}
