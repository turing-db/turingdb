#include <vector>
#include <string>

#include "NodeSearch.h"
#include "Explorator.h"
#include "SearchUtils.h"

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
#include "BioLog.h"

using namespace Log;
using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("biosearch");

    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("db", "import a database", "db_path");

    toolInit.init(argc, argv);

    DB* db = DB::create();

    std::vector<std::string> dbPaths;
    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "db") {
            dbPaths.emplace_back(option.second);
        }
    }

    if (dbPaths.empty()) {
        BioLog::printSummary();
        BioLog::destroy();
        PerfStat::destroy();
        return EXIT_SUCCESS;
    }

    {
        for (const auto& path : dbPaths) {
            DBLoader loader(db, path);
            const bool res = loader.load();
            bioassert(res);
        }
    }

    const NodeType* geneType = db->getNodeType(db->getString("DatabaseObjectPhysicalEntityGenomeEncodedEntityEntityWithAccessionedSequence"));
    bioassert(geneType);

    std::vector<Node*> genes;
    {
        TimerStat timerStat("Search gene nodes");
        BioLog::echo("Searching gene nodes");
        const std::vector<std::string> geneNames = {
            "CD44",
            "STAT3",
            "LCK",
            "GZMB",
            "SRGN",
            "CD6",
            "LGALS1",
            "TNFAIP6",
            "CCL5",
            "ALCAM",
            "LRP1",
            "IL1R1",
            "IL1B1",
            "SPP1",
            "CD36",
            "THBS1",
            "APOE",
            "LDLR",
            "MMP9",
            "VCAN",
            "A2M",
            "VSIG4",
            "C3",
            "IGHG1",
            "IGHG4",
            "IGKC",
            "IGHM",
            "GADD45A",
            "FANK1",
            "RYBP",
            "ITK",
            "KPNA2",
            "FOXP3",
            "HSPA1A",
            "CD28",
            "DUSP14"
        };

        {
            NodeSearch nodeSearch(db);
            nodeSearch.addAllowedType(geneType);
            nodeSearch.addProperty("speciesName", "Homo sapiens", true);
            nodeSearch.addProperty("referenceType", "ReferenceGeneProduct", true);

            for (const auto& name : geneNames) {
                nodeSearch.addProperty("displayName", name);
            }

            nodeSearch.run(genes);
        }

        // Write seed nodes report
        {
            Report seedReport(toolInit.getReportsDir()/"seeds.rpt", "Search seed nodes");
            auto& seedFile = seedReport.getStream();
            for (const Node* node : genes) {
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

        BioLog::echo("Number of gene names: "+std::to_string(geneNames.size()));
        BioLog::echo("Number of gene nodes found: "+std::to_string(genes.size()));
    }

    Network* subNet = nullptr;
    {
        TimerStat timerStat("Graph exploration");
        BioLog::echo("Graph exploration");
        Explorator explorator(db);
        explorator.addSeeds(genes);
        explorator.run();

        subNet = explorator.getResultNet();

        BioLog::echo("Pathways:");
        for (const Node* pathway : explorator.pathways()) {
            SearchUtils::printNode(pathway);
        }
        BioLog::echo("Number of pathways found: "+std::to_string(explorator.pathways().size()));
    }

    {
        BioLog::echo("Dumping result net as gml");
        GMLDumper dumper(subNet, toolInit.getOutputsDir()/"out.gml");
        const bool res = dumper.dump();
        bioassert(res);
    }

    BioLog::printSummary();
    BioLog::destroy();
    PerfStat::destroy();
    return EXIT_SUCCESS;
}
