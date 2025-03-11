#include "Graph.h"
#include "writers/DataPartBuilder.h"
#include "GraphMetadata.h"
#include "GraphDumper.h"
#include "comparators/GraphComparator.h"
#include "GraphLoader.h"
#include "LogUtils.h"
#include "Time.h"
#include "JobSystem.h"
#include "versioning/CommitBuilder.h"
#include "FileUtils.h"
#include "Neo4jImporter.h"
#include "Neo4j/Neo4JParserConfig.h"

using namespace db;

static std::unique_ptr<Graph> createSimpleGraph() {
    auto graph = std::make_unique<Graph>();
    const auto tx = graph->openWriteTransaction();
    auto commitBuilder = tx.prepareCommit();
    auto& builder = commitBuilder->newBuilder();
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
    const EntityID remy = builder.addNode(getLabelSet({"Person", "SoftwareEngineering", "Founder"}));
    const EntityID adam = builder.addNode(getLabelSet({"Person", "Bioinformatics", "Founder"}));
    const EntityID luc = builder.addNode(getLabelSet({"Person", "SoftwareEngineering"}));
    const EntityID maxime = builder.addNode(getLabelSet({"Person", "Bioinformatics"}));
    const EntityID martina = builder.addNode(getLabelSet({"Person", "Bioinformatics"}));

    // Interests
    const EntityID ghosts = builder.addNode(getLabelSet({"Interest"}));
    const EntityID bio = builder.addNode(getLabelSet({"Interest"}));
    const EntityID cooking = builder.addNode(getLabelSet({"Interest"}));
    const EntityID paddle = builder.addNode(getLabelSet({"Interest"}));
    const EntityID animals = builder.addNode(getLabelSet({"Interest"}));
    const EntityID computers = builder.addNode(getLabelSet({"Interest"}));
    const EntityID eighties = builder.addNode(getLabelSet({"Interest"}));

    // Property types
    const PropertyType name = proptypes.getOrCreate("name", types::String::_valueType);

    // Edges
    const EdgeRecord e01 = builder.addEdge(knowsWellID, remy, adam);
    const EdgeRecord e02 = builder.addEdge(knowsWellID, adam, remy);
    const EdgeRecord e03 = builder.addEdge(interestedInID, remy, ghosts);
    const EdgeRecord e04 = builder.addEdge(interestedInID, remy, computers);
    const EdgeRecord e05 = builder.addEdge(interestedInID, remy, eighties);
    const EdgeRecord e06 = builder.addEdge(interestedInID, adam, bio);
    const EdgeRecord e07 = builder.addEdge(interestedInID, adam, cooking);
    const EdgeRecord e08 = builder.addEdge(interestedInID, luc, animals);
    const EdgeRecord e09 = builder.addEdge(interestedInID, luc, computers);
    const EdgeRecord e10 = builder.addEdge(interestedInID, maxime, bio);
    const EdgeRecord e11 = builder.addEdge(interestedInID, maxime, paddle);
    const EdgeRecord e12 = builder.addEdge(interestedInID, martina, cooking);

    // Node Properties
    builder.addNodeProperty<types::String>(remy, name._id, "Remy");
    builder.addNodeProperty<types::String>(adam, name._id, "Adam");
    builder.addNodeProperty<types::String>(luc, name._id, "Luc");
    builder.addNodeProperty<types::String>(maxime, name._id, "Maxime");
    builder.addNodeProperty<types::String>(martina, name._id, "Martina");
    builder.addNodeProperty<types::String>(ghosts, name._id, "Ghosts");
    builder.addNodeProperty<types::String>(bio, name._id, "Bio");
    builder.addNodeProperty<types::String>(cooking, name._id, "Cooking");
    builder.addNodeProperty<types::String>(paddle, name._id, "Paddle");
    builder.addNodeProperty<types::String>(animals, name._id, "Animals");
    builder.addNodeProperty<types::String>(computers, name._id, "Computers");
    builder.addNodeProperty<types::String>(eighties, name._id, "Eighties");

    // Edge Properties
    builder.addEdgeProperty<types::String>(e01, name._id, "Remy -> Adam");
    builder.addEdgeProperty<types::String>(e02, name._id, "Adam -> Remy");
    builder.addEdgeProperty<types::String>(e03, name._id, "Remy -> Ghosts");
    builder.addEdgeProperty<types::String>(e04, name._id, "Remy -> Computers");
    builder.addEdgeProperty<types::String>(e05, name._id, "Remy -> Eighties");
    builder.addEdgeProperty<types::String>(e06, name._id, "Adam -> Bio");
    builder.addEdgeProperty<types::String>(e07, name._id, "Adam -> Cooking");
    builder.addEdgeProperty<types::String>(e08, name._id, "Luc -> Animals");
    builder.addEdgeProperty<types::String>(e09, name._id, "Luc -> Computers");
    builder.addEdgeProperty<types::String>(e10, name._id, "Maxime -> Bio");
    builder.addEdgeProperty<types::String>(e11, name._id, "Maxime -> Paddle");
    builder.addEdgeProperty<types::String>(e12, name._id, "Martina -> Cooking");

    auto jobSystem = JobSystem::create();
    graph->commit(std::move(commitBuilder), *jobSystem);
    return graph;
}

bool testGraph(const Graph& graph, const fs::Path& path) {
    fmt::print("- Dumping graph to: {}\n", path.c_str());

    {
        const auto t0 = Clock::now();
        if (auto res = GraphDumper::dump(graph, path); !res) {
            fmt::print("{}\n", res.error().fmtMessage());
            return false;
        }
        const auto t1 = Clock::now();
        logt::ElapsedTime(duration<Seconds>(t0, t1), "s");
    }

    fmt::print("- Loading graph from: {}\n", path.c_str());

    const auto t0 = Clock::now();
    auto loadedGraph = std::make_unique<Graph>();
    auto loadedGraphRes = GraphLoader::load(loadedGraph.get(), path);
    if (!loadedGraphRes) {
        fmt::print("{}\n", loadedGraphRes.error().fmtMessage());
        return false;
    }
    const auto t1 = Clock::now();
    logt::ElapsedTime(duration<Seconds>(t0, t1), "s");


    if (!GraphComparator::same(graph, *loadedGraph)) {
        fmt::print("Loaded graph is not the same as the one dumped\n");
        return false;
    }

    return true;
}

int main() {
    {
        // Load & dump simple graph
        const fs::Path path {SAMPLE_DIR "/simple-graph"};

        auto graph = createSimpleGraph();

        if (path.exists()) {
            // Removing existing dir
            if (auto res = path.rm(); !res) {
                fmt::print("{}\n", res.error().fmtMessage());
                return 1;
            }
        }

        if (!testGraph(*graph, path)) {
            return 1;
        }
    }

    {
        // Dump pole
        const fs::Path path {SAMPLE_DIR "/pole"};

        auto jobSystem = JobSystem::create();

        auto graph = std::make_unique<Graph>();
        const std::string turingHome = std::getenv("TURING_HOME");
        const fs::Path jsonDir = fs::Path {turingHome} / "neo4j" / "pole-db";

        if (!Neo4jImporter::importJsonDir(*jobSystem,
                                          graph.get(),
                                          db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                          db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                          {
                                              ._jsonDir = FileUtils::Path {jsonDir.get()},
                                          })) {
            fmt::print("Could not load Pole\n");
            return 1;
        }

        if (path.exists()) {
            // Removing existing dir
            if (auto res = path.rm(); !res) {
                fmt::print("{}\n", res.error().fmtMessage());
                return 1;
            }
        }

        if (!testGraph(*graph, path)) {
            return 1;
        }
    }

    {
        // Dump reactome
        auto t0 = Clock::now();
        const fs::Path path {SAMPLE_DIR "/reactome"};

        auto jobSystem = JobSystem::create();

        auto graph = std::make_unique<Graph>();
        const std::string turingHome = std::getenv("HOME");
        const fs::Path jsonDir = fs::Path {turingHome} / "graphs_v2" / "reactome";

        if (!Neo4jImporter::importJsonDir(*jobSystem,
                                          graph.get(),
                                          db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                          db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                          {
                                              ._jsonDir = FileUtils::Path {jsonDir.get()},
                                          })) {
            fmt::print("Could not load Reactome\n");
            return 1;
        }

        const auto t1 = Clock::now();
        logt::ElapsedTime(duration<Seconds>(t0, t1), "s");

        if (path.exists()) {
            // Removing existing dir
            if (auto res = path.rm(); !res) {
                fmt::print("{}\n", res.error().fmtMessage());
                return 1;
            }
        }

        if (!testGraph(*graph, path)) {
            return 1;
        }
    }

    // {
    //     // Dump ckg
    //     const fs::Path path {SAMPLE_DIR "/ckg"};

    //     JobSystem jobSystem {1};
    //     jobSystem.initialize();

    //     auto graph = std::make_unique<Graph>();
    //     const std::string turingHome = std::getenv("HOME");
    //     const fs::Path jsonDir = fs::Path {turingHome} / "graphs_v2" / "ckg";

    //     if (!Neo4jImporter::importJsonDir(jobSystem,
    //                                  graph.get(),
    //                                  db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
    //                                  db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
    //                                  {
    //                                      ._jsonDir = FileUtils::Path {jsonDir.get()},
    //                                  })) {
    //         fmt::print("Could not load CKG\n");
    //         return 1;
    //     }

    //     if (path.exists()) {
    //         // Removing existing dir
    //         if (auto res = path.rm(); !res) {
    //             fmt::print("{}\n", res.error().fmtMessage());
    //             return 1;
    //         }
    //     }

    //    if (!testGraph(*graph, path)) {
    //        return 1;
    //    }
    // }

    return 0;
}
