#include "Graph.h"
#include "DataPartBuilder.h"
#include "GraphMetadata.h"
#include "GraphDumper.h"
#include "JobSystem.h"

using namespace db;

static std::unique_ptr<Graph> createSimpleGraph() {
    auto graph = std::make_unique<Graph>();
    auto builder = graph->newPartWriter();
    auto* metadata = graph->getMetadata();
    auto& labels = metadata->labels();
    auto& labelsets = metadata->labelsets();
    auto& proptypes = metadata->propTypes();
    auto& edgetypes = metadata->edgeTypes();

    const auto getLabelSet = [&](std::initializer_list<std::string> labelList) {
        LabelSet lset;
        for (const auto& l : labelList) {
            lset.set(labels.getOrCreate(l));
        }
        return labelsets.getOrCreate(lset);
    };

    // EdgeTypes
    const EdgeTypeID knowsWellID = edgetypes.getOrCreate("KNOWS_WELL");
    const EdgeTypeID interestedInID = edgetypes.getOrCreate("INTERESTED_IN");

    // Persons
    const EntityID remy = builder->addNode(getLabelSet({"Person", "SoftwareEngineering", "Founder"}));
    const EntityID adam = builder->addNode(getLabelSet({"Person", "Bioinformatics", "Founder"}));
    const EntityID luc = builder->addNode(getLabelSet({"Person", "SoftwareEngineering"}));
    const EntityID maxime = builder->addNode(getLabelSet({"Person", "Bioinformatics"}));
    const EntityID martina = builder->addNode(getLabelSet({"Person", "Bioinformatics"}));

    // Interests
    const EntityID ghosts = builder->addNode(getLabelSet({"Interest"}));
    const EntityID bio = builder->addNode(getLabelSet({"Interest"}));
    const EntityID cooking = builder->addNode(getLabelSet({"Interest"}));
    const EntityID paddle = builder->addNode(getLabelSet({"Interest"}));
    const EntityID animals = builder->addNode(getLabelSet({"Interest"}));
    const EntityID computers = builder->addNode(getLabelSet({"Interest"}));
    const EntityID eighties = builder->addNode(getLabelSet({"Interest"}));

    // Property types
    const PropertyType name = proptypes.getOrCreate("name", types::String::_valueType);

    // Edges
    const EdgeRecord e01 = builder->addEdge(knowsWellID, remy, adam);
    const EdgeRecord e02 = builder->addEdge(knowsWellID, adam, remy);
    const EdgeRecord e03 = builder->addEdge(interestedInID, remy, ghosts);
    const EdgeRecord e04 = builder->addEdge(interestedInID, remy, computers);
    const EdgeRecord e05 = builder->addEdge(interestedInID, remy, eighties);
    const EdgeRecord e06 = builder->addEdge(interestedInID, adam, bio);
    const EdgeRecord e07 = builder->addEdge(interestedInID, adam, cooking);
    const EdgeRecord e08 = builder->addEdge(interestedInID, luc, animals);
    const EdgeRecord e09 = builder->addEdge(interestedInID, luc, computers);
    const EdgeRecord e10 = builder->addEdge(interestedInID, maxime, bio);
    const EdgeRecord e11 = builder->addEdge(interestedInID, maxime, paddle);
    const EdgeRecord e12 = builder->addEdge(interestedInID, martina, cooking);

    // Node Properties
    builder->addNodeProperty<types::String>(remy, name._id, "Remy");
    builder->addNodeProperty<types::String>(adam, name._id, "Adam");
    builder->addNodeProperty<types::String>(luc, name._id, "Luc");
    builder->addNodeProperty<types::String>(maxime, name._id, "Maxime");
    builder->addNodeProperty<types::String>(martina, name._id, "Martina");
    builder->addNodeProperty<types::String>(ghosts, name._id, "Ghosts");
    builder->addNodeProperty<types::String>(bio, name._id, "Bio");
    builder->addNodeProperty<types::String>(cooking, name._id, "Cooking");
    builder->addNodeProperty<types::String>(paddle, name._id, "Paddle");
    builder->addNodeProperty<types::String>(animals, name._id, "Animals");
    builder->addNodeProperty<types::String>(computers, name._id, "Computers");
    builder->addNodeProperty<types::String>(eighties, name._id, "Eighties");

    // Edge Properties
    builder->addEdgeProperty<types::String>(e01, name._id, "Remy -> Adam");
    builder->addEdgeProperty<types::String>(e02, name._id, "Adam -> Remy");
    builder->addEdgeProperty<types::String>(e03, name._id, "Remy -> Ghosts");
    builder->addEdgeProperty<types::String>(e04, name._id, "Remy -> Computers");
    builder->addEdgeProperty<types::String>(e05, name._id, "Remy -> Eighties");
    builder->addEdgeProperty<types::String>(e06, name._id, "Adam -> Bio");
    builder->addEdgeProperty<types::String>(e07, name._id, "Adam -> Cooking");
    builder->addEdgeProperty<types::String>(e08, name._id, "Luc -> Animals");
    builder->addEdgeProperty<types::String>(e09, name._id, "Luc -> Computers");
    builder->addEdgeProperty<types::String>(e10, name._id, "Maxime -> Bio");
    builder->addEdgeProperty<types::String>(e11, name._id, "Maxime -> Paddle");
    builder->addEdgeProperty<types::String>(e12, name._id, "Martina -> Cooking");

    JobSystem jobSystem;
    jobSystem.initialize();
    builder->commit(jobSystem);
    return graph;
}

int main() {
    const fs::Path path {SAMPLE_DIR "/simple-graph"};

    auto graph = createSimpleGraph();

    if (path.exists()) {
        // Removing existing dir
        if (auto res = path.rm(); !res) {
            fmt::print("{}\n", res.error().fmtMessage());
            return 1;
        }
    }

    if (auto res = GraphDumper::dump(*graph, path); !res) {
        fmt::print("{}\n", res.error().fmtMessage());
        return 1;
    }

    return 0;
}
