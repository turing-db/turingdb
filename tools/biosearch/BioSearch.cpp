#include <vector>
#include <string>
#include <fstream>

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

#include "MsgCommon.h"
#include "BioAssert.h"
#include "BioLog.h"

using namespace Log;
using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("biosearch");

    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("db", "Import a database", "db_path");
    argParser.addOption("seeds", "File containing a list of seed nodes", "seeds_path");
    argParser.addOption("seed", "Add a seed node name", "name");
    argParser.addOption("exclude", "Exclude nodes with a given name", "name");
    argParser.addOption("target", "Add a schemaClass for target nodes (Drugs by default)", "schemaClass");
    argParser.addOption("traverse_targets", "Traverse target nodes during exploration");
    argParser.addOption("no_sets", "Exclude sets of entities (CandidateSet, DefinedSet..etc)");
    argParser.addOption("max_dist", "Maximum distance", "distance");
    argParser.addOption("no_default_excluded", "Do not use default exclusion rules for node names");
    argParser.addOption("no_default_excluded_class", "Do not use default exclusion rules for schemaClass");
    argParser.addOption("exclude_class", "Exclude a schemaClass", "schemaClass");
    argParser.addOption("no_pathways", "Do not traverse pathways");
    argParser.addOption("max_degree", "Maximum degree of a node", "degree");
    argParser.addOption("use_failed_reaction", "Use FailedReaction information");

    toolInit.init(argc, argv);

    std::vector<std::string> seedNames;

    // Parse options
    std::vector<std::string> dbPaths;
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

    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "db") {
            dbPaths.emplace_back(option.second);
        } else if (optName == "seeds") {
            seedFilePath = option.second;
        } else if (optName == "exclude") {
            excludedNames.push_back(option.second);
        } else if (optName == "target") {
            targetClasses.push_back(option.second);
        } else if (optName == "traverse_targets") {
            traverseTargets = true;
        } else if (optName == "max_dist") {
            maxDistance = std::stoul(option.second);
        } else if (optName == "no_default_excluded") {
            enableDefaultExcludedNames = false;
        } else if (optName == "exclude_class") {
            excludedClasses.push_back(option.second);
        } else if (optName == "no_default_excluded_class") {
            enableDefaultExcludedClasses = false;
        } else if (optName == "no_pathways") {
            traversePathways = false;
        } else if (optName == "seed") {
            seedNames.push_back(option.second);
        } else if (optName == "max_degree") {
            maxDegree = std::stoul(option.second);
        } else if (optName == "no_sets") {
            traverseSets = false;
        } else if (optName == "use_failed_reaction") {
            traverseFailedReaction = true;
        }
    }

    if (dbPaths.empty() || (seedFilePath.empty() && seedNames.empty())) {
        BioLog::log(msg::ERROR_EXPECTED_OPTIONS() << "-db and -seeds or -seed");
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
            BioLog::log(msg::ERROR_FAILED_TO_OPEN_FOR_READ() << seedFilePath);
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
            BioLog::echo("Searching seed nodes");
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
            Report seedReport(toolInit.getReportsDir()/"seeds.rpt", "Search seed nodes");
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

        BioLog::echo("Number of seed names: "+std::to_string(seedNames.size()));
        BioLog::echo("Number of seed nodes found: "+std::to_string(seeds.size()));
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
                BioLog::echo("Added target schemaClass "+targetClassName);
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
            BioLog::echo("Graph exploration");
            explorator.run();
        }

        subNet = explorator.getResultNet();

        const auto displayNameProp = db->getString("displayName");

        {
            Report resultsReport(toolInit.getReportsDir()/"results.rpt", "Result nodes found");
            auto& resultsFile = resultsReport.getStream();

            for (const ExploratorTreeNode* target : explorator.targets()) {
                SearchUtils::printNode(target->getNode(), resultsFile);
                SearchUtils::printPath(target, displayNameProp, resultsFile);
                resultsFile << "\n";
            }
        }

        BioLog::echo("\nNumber of target nodes found: " + std::to_string(explorator.targets().size()));
    }

    {
        BioLog::echo("Dumping result net as gml");
        GMLDumper dumper(subNet, toolInit.getOutputsDir()/"out.gml");
        [[maybe_unused]] const bool res = dumper.dump();
        bioassert(res);
    }

    return EXIT_SUCCESS;
}
