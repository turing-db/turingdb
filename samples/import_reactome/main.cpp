#include "DB.h"
#include "EdgeView.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "Neo4jImporter.h"
#include "Neo4j/ParserConfig.h"
#include "NodeView.h"
#include "Reader.h"
#include "Time.h"

#include <iostream>

using namespace db;

int main() {
    JobSystem jobSystem;
    jobSystem.initialize();

    auto database = std::make_unique<DB>();

    const FileUtils::Path jsonDir = "/net/db/reactome/json/";

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

    auto access = database->access();
    auto reader = access.getReader();

    std::string_view apoe4ReactomeID = "R-HSA-9711070";
    PropertyType displayNameType = access.getPropertyType("displayName (String)");
    PropertyType stIDType = access.getPropertyType("stId (String)");

    auto it = reader.scanNodeProperties<types::String>(stIDType._id).begin();
    const auto findReactomeID = [&]() {
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
        throw;
    };

    t0 = Clock::now();
    EntityID apoeID = findReactomeID();
    t1 = Clock::now();
    std::cout << "found APOE-4 in: " << duration<Milliseconds>(t0, t1) << " ms" << std::endl;

    t0 = Clock::now();
    NodeView apoe = reader.getNodeView(apoeID);
    t1 = Clock::now();
    std::cout << "Got APOE-4 view in: " << duration<Microseconds>(t0, t1) << " us" << std::endl;

    LabelsetID labelsetID = apoe.labelset();
    const Labelset& labelset = database->getLabelset(labelsetID);
    std::vector<LabelID> labelIDs;
    labelset.decompose(labelIDs);
    std::cout << "APOE-4 has labels: " << std::endl;
    for (const LabelID id : labelIDs) {
        std::cout << "  - " << access.getLabelName(id) << std::endl;
    }

    const auto& apoeEdges = apoe.edges();
    const auto& apoeProperties = apoe.properties();
    const auto printProperties = [&access](const EntityPropertyView& view) {
        if (!view.uint64s().empty()) {
            std::cout << "  #  UInt64 properties" << std::endl;
            for (const auto& prop : view.uint64s()) {
                const std::string_view ptName = access.getPropertyTypeName(prop._id);
                std::cout << "    - " << ptName << ": " << prop.get<types::UInt64>() << std::endl;
            }
        }

        if (!view.int64s().empty()) {
            std::cout << "  #  Int64 properties" << std::endl;
            for (const auto& prop : view.int64s()) {
                const std::string_view ptName = access.getPropertyTypeName(prop._id);
                std::cout << "    - " << ptName << ": " << prop.get<types::Int64>() << std::endl;
            }
        }

        if (!view.doubles().empty()) {
            std::cout << "  #  Double properties" << std::endl;
            for (const auto& prop : view.doubles()) {
                const std::string_view ptName = access.getPropertyTypeName(prop._id);
                std::cout << "    - " << ptName << ": " << prop.get<types::Double>() << std::endl;
            }
        }

        if (!view.strings().empty()) {
            std::cout << "  #  String properties" << std::endl;
            for (const auto& prop : view.strings()) {
                const std::string_view ptName = access.getPropertyTypeName(prop._id);
                std::cout << "    - " << ptName << ": " << prop.get<types::String>() << std::endl;
            }
        }

        if (!view.bools().empty()) {
            std::cout << "  #  Bool properties" << std::endl;
            for (const auto& prop : view.bools()) {
                const std::string_view ptName = access.getPropertyTypeName(prop._id);
                std::cout << "    - " << ptName << ": " << prop.get<types::Bool>() << std::endl;
            }
        }
    };

    printProperties(apoeProperties);

    std::cout << "APOE has " << apoeEdges.getOutEdgeCount() << " out edges" << std::endl;
    for (const auto& edge : apoe.edges().outEdges()) {
        const EdgeView edgeView = reader.getEdgeView(edge._edgeID);
        const NodeView target = reader.getNodeView(edge._otherID);
        const types::String::Primitive& displayName =
            target.properties().getProperty<types::String>(displayNameType._id);
        std::cout << "  -> " << edge._otherID.getValue()
                  << " with " << edgeView.properties().getCount() << " properties"
                  << std::endl;
        printProperties(edgeView.properties());
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
        printProperties(edgeView.properties());
        std::cout << "       * " << displayName << std::endl;
    }

    std::stringstream report;
    access.getReport(report);
    std::cout << report.view() << std::endl;

    return 0;
}
