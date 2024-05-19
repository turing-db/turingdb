#include <vector>
#include <string>
#include <fstream>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "NodeSearch.h"
#include "Explorator.h"
#include "SearchUtils.h"
#include "ExploratorTree.h"

#include "GMLDumper.h"

#include "Report.h"

#include "ToolInit.h"
#include "PerfStat.h"
#include "TimerStat.h"

#include "DB.h"
#include "DBLoader.h"
#include "Node.h"
#include "NodeType.h"

#include "BioAssert.h"
#include "LogUtils.h"

using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("biosearch");

    std::vector<std::string> dbPaths;
    std::vector<std::string> seedNames;
    std::string seedFilePath;
    std::vector<std::string> excludedNames;
    std::vector<std::string> excludedClasses;
    std::vector<std::string> targetClasses;
    size_t maxDistance = 5;
    size_t maxDegree = 0;
    bool traverseTargets = false;
    bool enableDefaultExcludedNames = true;
    bool enableDefaultExcludedClasses = true;
    bool traversePathways = true;
    bool traverseSets = true;
    bool traverseFailedReaction = false;

    auto& argParser = toolInit.getArgParser();

    argParser.add_argument("-db")
             .help("Import a database")
             .nargs(1)
             .append()
             .store_into(dbPaths);

    argParser.add_argument("-seeds")
             .help("File containing a list of seed nodes")
             .nargs(1)
             .store_into(seedFilePath);

    argParser.add_argument("-seed")
             .help("Add a seed node name")
             .nargs(1)
             .append()
             .store_into(seedNames);

    argParser.add_argument("-exclude")
             .help("Exclude nodes with a given name")
             .nargs(1)
             .append()
             .store_into(excludedNames);

    argParser.add_argument("-target")
             .help("Add a schemaClass for target nodes (Drugs by default)")
             .nargs(1)
             .append()
             .store_into(targetClasses);

    argParser.add_argument("-traverse_targets")
             .help("Traverse target nodes during exploration")
             .nargs(0)
             .action([&](const auto&){ traverseTargets = false; });

    argParser.add_argument("-no_sets")
             .help("Exclude sets of entities (CandidateSet, DefinedSet..etc)")
             .nargs(0)
             .action([&](const auto&){ traverseSets = false; });

    argParser.add_argument("-max_dist")
             .help("Maximum distance")
             .nargs(1)
             .store_into(maxDistance);

    argParser.add_argument("-no_default_excluded")
             .help("Do not use default exclusion rules for node names")
             .nargs(0)
             .action([&](const auto&){ enableDefaultExcludedNames = false; });

    argParser.add_argument("-no_default_excluded_class")
             .help("Do not use default exclusion rules for schemaClass")
             .nargs(0)
             .action([&](const auto&){ enableDefaultExcludedClasses = false; });

    argParser.add_argument("-exclude_class")
             .help("Exclude a schemaClass")
             .nargs(1)
             .append()
             .store_into(excludedClasses);

    argParser.add_argument("-no_pathways")
             .help("Do not traverse pathways")
             .nargs(0)
             .action([&](const auto&){ traversePathways = false; });

    argParser.add_argument("-max_degree")
             .help("Maximum degree of a node")
             .nargs(1)
             .store_into(maxDegree);

    argParser.add_argument("-use_failed_reaction")
             .help("Use FailedReaction information")
             .nargs(0)
             .store_into(traverseFailedReaction);

    toolInit.init(argc, argv);

    if (dbPaths.empty() || (seedFilePath.empty() && seedNames.empty())) {
        spdlog::error("The options -db and -seeds or -seed are expected");
        return EXIT_FAILURE;
    }

    // Import db
    DB* db = DB::create();
    {
        for (const auto& path : dbPaths) {
            DBLoader loader(db, path);
            [[maybe_unused]] const bool res = loader.load();
            bioassert(res);
        }
    }

    // Read seed names
    if (!seedFilePath.empty()) {
        std::ifstream seedFile(seedFilePath);
        if (!seedFile.is_open()) {
            logt::CanNotRead(seedFilePath);
            return EXIT_FAILURE;
        }

        std::string name;
        while (seedFile >> name) {
            seedNames.emplace_back(name);
        }
    }

    // Search seed nodes
    std::vector<Node*> seeds;
    {
        {
            TimerStat timerStat("Search seed nodes");
            spdlog::info("Searching seed nodes");
            NodeSearch nodeSearch(db);
            nodeSearch.addProperty("speciesName", "Homo sapiens", NodeSearch::MatchType::EXACT);
            nodeSearch.addProperty("schemaClass", "EntityWithAccessionedSequence", NodeSearch::MatchType::EXACT);
            nodeSearch.addProperty("referenceType", "ReferenceGeneProduct", NodeSearch::MatchType::EXACT);

            for (const auto& name : seedNames) {
                nodeSearch.addProperty("displayName", name, NodeSearch::MatchType::PREFIX_AND_LOC);
            }

            nodeSearch.run(seeds);
        }

        // Write seed nodes report
        {
            Report seedReport(toolInit.getReportsDir()+"/seeds.rpt", "Search seed nodes");
            auto& seedFile = seedReport.getStream();
            for (const Node* node : seeds) {
                seedFile << "========\n";
                for (const auto& [propType, value] : node->properties()) {
                    if (propType->getValueType().isString()) {
                        seedFile << propType->getName().toStdString()
                                 << " : "
                                 << value.getString()
                                 << "\n";
                    }
                }
                seedFile << "\n";
            }
        }

        spdlog::info("Number of seed names: {}", seedNames.size());
        spdlog::info("Number of seed nodes found: {}", seeds.size());
    }

    // Graph exploration
    Network* subNet = nullptr;
    {
        Explorator explorator(db);
        explorator.addSeeds(seeds);
        explorator.setMaximumDistance(maxDistance);
        explorator.setTraverseTargets(traverseTargets);
        explorator.setTraversePathways(traversePathways);
        explorator.setTraverseSets(traverseSets);
        explorator.setTraverseFailedReaction(traverseFailedReaction);

        if (maxDegree > 0) {
            explorator.setMaximumDegree(maxDegree);
        }

        // Add target schema classes
        if (!targetClasses.empty()) {
            explorator.setNoDefaultTargets();
            for (const auto& targetClassName : targetClasses) {
                if (targetClassName.empty()) {
                    continue;
                }
                explorator.addTargetClass(targetClassName);
                spdlog::info("Added target schemaClass {}", targetClassName);
            }
        }

        // Add excluded names
        if (enableDefaultExcludedNames) {
            explorator.addDefaultExcludedNames();
        }

        for (const auto& excludedName : excludedNames) {
            explorator.addExcludedName(excludedName);
        }

        // Add excluded classes
        if (enableDefaultExcludedClasses) {
            explorator.addDefaultExcludedClasses();
        }

        for (const auto& excludedClass : excludedClasses) {
            explorator.addExcludedClass(excludedClass);
        }

        {
            TimerStat timerStat("Graph exploration");
            spdlog::info("Graph exploration");
            explorator.run();
        }

        subNet = explorator.getResultNet();

        const auto displayNameProp = db->getString("displayName");

        {
            Report resultsReport(toolInit.getReportsDir()+"/results.rpt", "Result nodes found");
            auto& resultsFile = resultsReport.getStream();

            for (const ExploratorTreeNode* target : explorator.targets()) {
                SearchUtils::printNode(target->getNode(), resultsFile);
                SearchUtils::printPath(target, displayNameProp, resultsFile);
                resultsFile << "\n";
            }
        }

        spdlog::info("\nNumber of target nodes found: {}", explorator.targets().size());
    }

    {
        spdlog::info("Dumping result net as gml");
        GMLDumper dumper(subNet, toolInit.getOutputsDir()+"/out.gml");
        [[maybe_unused]] const bool res = dumper.dump();
        bioassert(res);
    }

    return EXIT_SUCCESS;
}
