#include "Graph.h"
#include "GraphView.h"
#include "GraphReader.h"
#include "EdgeView.h"
#include "FileUtils.h"
#include "GraphMetadata.h"
#include "JobSystem.h"
#include "Neo4j/ParserConfig.h"
#include "Neo4jImporter.h"
#include "NodeView.h"
#include "PerfStat.h"
#include "Time.h"

#include <iostream>

using namespace db;

int main() {
    JobSystem jobSystem;
    jobSystem.initialize();
    PerfStat::init("import_reactome.perf");

    auto database = std::make_unique<DB>();
    const PropertyTypeMap& propTypes = database->getMetadata()->propTypes();
    const LabelMap& labels = database->getMetadata()->labels();
    LabelSetMap& labelsets = database->getMetadata()->labelsets();

    const FileUtils::Path jsonDir = "/home/luclabarriere/jsonReactome";

    auto t0 = Clock::now();
    Neo4jImporter::importJsonDir(jobSystem,
                                 database.get(),
                                 nodeCountLimit,
                                 edgeCountLimit,
                                 {
                                     ._jsonDir = jsonDir,
                                 });
    auto t1 = Clock::now();

    std::cout << "Parsing: " << duration<Seconds>(t0, t1) << " s" << std::endl;

    const auto access = database->access();
    const auto reader = access.read();

    std::string_view apoe4ReactomeID = "R-HSA-9711070";
    PropertyType displayNameType = propTypes.get("displayName (String)");
    PropertyType stIDType = propTypes.get("stId (String)");

    const auto findReactomeID = [&]() {
        LabelSet labelset;
        labelset.set(labels.get("DatabaseObject"));
        labelset.set(labels.get("PhysicalEntity"));
        labelset.set(labels.get("GenomeEncodedEntity"));
        labelset.set(labels.get("EntityWithAccessionedSequence"));

        auto it = reader.scanNodePropertiesByLabel<types::String>(stIDType._id, &labelset).begin();
        size_t i = 0;
        for (; it.isValid(); it.next()) {
            const types::String::Primitive& stID = it.get();
            if (stID == apoe4ReactomeID) {
                EntityID apoeID = it.getCurrentNodeID();
                std::cout << "Found APOE-4 at ID: " << apoeID.getValue()
                          << " in " << i << " string comparisons"
                          << std::endl;
                return apoeID;
            }
            i++;
        }
        std::cout << "Could not find APOE-4" << std::endl;
        return EntityID {};
    };

    t0 = Clock::now();
    EntityID apoeID = findReactomeID();
    t1 = Clock::now();
    std::cout << "Found APOE-4 in: " << duration<Milliseconds>(t0, t1) << " ms" << std::endl;

    t0 = Clock::now();
    NodeView apoe = reader.getNodeView(apoeID);
    t1 = Clock::now();
    std::cout << "Got APOE-4 view in: " << duration<Microseconds>(t0, t1) << " us" << std::endl;

    LabelSetID labelsetID = apoe.labelset();
    const LabelSet& labelset = labelsets.getValue(labelsetID);

    std::vector<LabelID> labelIDs;
    labelset.decompose(labelIDs);
    std::cout << "APOE-4 has labels: " << std::endl;
    for (const LabelID id : labelIDs) {
        std::cout << "  - " << labels.getName(id) << std::endl;
    }

    const auto& apoeEdges = apoe.edges();

    std::cout << "APOE has " << apoeEdges.getOutEdgeCount() << " out edges" << std::endl;
    for (const auto& edge : apoe.edges().outEdges()) {
        const EdgeView edgeView = reader.getEdgeView(edge._edgeID);
        const NodeView target = reader.getNodeView(edge._otherID);
        const types::String::Primitive& displayName =
            target.properties().getProperty<types::String>(displayNameType._id);
        std::cout << "  -> " << edge._otherID.getValue()
                  << " with " << edgeView.properties().getCount() << " properties"
                  << std::endl;
        std::cout << "       * " << displayName << std::endl;
    }

    std::cout << "APOE has " << apoeEdges.getInEdgeCount() << " in edges" << std::endl;
    for (const auto& edge : apoe.edges().inEdges()) {
        const EdgeView edgeView = reader.getEdgeView(edge._edgeID);
        const NodeView source = reader.getNodeView(edge._otherID);
        const types::String::Primitive& displayName =
            source.properties().getProperty<types::String>(displayNameType._id);
        std::cout << "  <- " << edge._otherID.getValue()
                  << " with " << edgeView.properties().getCount() << " properties"
                  << std::endl;
        std::cout << "       * " << displayName << std::endl;
    }

    PerfStat::destroy();
    return 0;
}
