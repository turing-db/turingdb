#include "DB.h"
#include "DBAccess.h"
#include "DBMetadata.h"
#include "DBReport.h"
#include "FileUtils.h"
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
    PerfStat::init("import_pole.perf");

    auto database = std::make_unique<DB>();
    const PropertyTypeMap& propTypes = database->getMetadata()->propTypes();
    const std::string turingHome = std::getenv("TURING_HOME");
    const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / "pole-db";

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

    std::string_view address = "33 Plover Drive";
    PropertyType addressType = propTypes.get("address (String)");
    auto it = access.scanNodeProperties<types::String>(addressType._id).begin();

    const auto findNodeID = [&]() {
        for (; it.isValid(); it.next()) {
            const types::String::Primitive& v = it.get();
            if (v == address) {
                EntityID nodeID = it.getCurrentNodeID();
                std::cout << "Found address at ID: " << nodeID.getValue() << std::endl;
                return nodeID;
            }
        }
        std::cout << "Could not find location 33 Plover Drive" << std::endl;
        return EntityID {};
    };

    t0 = Clock::now();
    EntityID nodeID = findNodeID();
    t1 = Clock::now();
    std::cout << "Found Location in: " << duration<Microseconds>(t0, t1) << " us" << std::endl;

    t0 = Clock::now();
    NodeView node = access.getNodeView(nodeID);
    t1 = Clock::now();
    std::cout << "Got node view in: " << duration<Microseconds>(t0, t1) << " us" << std::endl;

    const auto& nodeEdges = node.edges();
    const auto& nodeProperties = node.properties();
    const types::String::Primitive& addressValue =
        nodeProperties.getProperty<types::String>(addressType._id);

    std::cout << "Location has address: " << addressValue << std::endl;
    std::cout << "And other string props: " << std::endl;

    const auto& strings = nodeProperties.strings();
    for (const auto& prop : strings) {
        const auto& v = prop.get<types::String>();
        std::cout << "  - " << prop._id.getValue() << ": " << v << std::endl;
    }

    std::cout << "Location has " << nodeEdges.getOutEdgeCount() << " out edges" << std::endl;
    for (const auto& edge : nodeEdges.outEdges()) {
        NodeView target = access.getNodeView(edge._otherID);
        const auto& targetProps = target.properties();

        std::cout << "  -> " << edge._otherID.getValue() << std::endl;

        for (const auto& propView : targetProps.strings()) {
            const auto& propName = propTypes.getName(propView._id);
            std::cout << "    - " << propName << ": " << propView.get<types::String>()
                      << std::endl;
        }
    }

    std::cout << "Location has " << nodeEdges.getInEdgeCount() << " in edges" << std::endl;
    for (const auto& edge : node.edges().inEdges()) {
        NodeView source = access.getNodeView(edge._otherID);
        const auto& sourceProps = source.properties();

        std::cout << "  <- " << edge._otherID.getValue() << std::endl;

        for (const auto& propView : sourceProps.strings()) {
            const auto& propName = propTypes.getName(propView._id);
            std::cout << "    - " << propName << ": " << propView.get<types::String>()
                      << std::endl;
        }
    }

    std::stringstream report;
    DBReport::getReport(database->access(), report);
    std::cout << report.view() << std::endl;

    PerfStat::destroy();
    return 0;
}
