#include "Graph.h"
#include "GraphView.h"
#include "GraphReader.h"
#include "GraphMetadata.h"
#include "GraphReport.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "Neo4j/Neo4JParserConfig.h"
#include "Neo4jImporter.h"
#include "NodeView.h"
#include "PerfStat.h"
#include "Time.h"
#include "LogSetup.h"

#include <iostream>

using namespace db;

int main() {
    JobSystem jobSystem;
    LogSetup::setupLogFileBacked(SAMPLE_NAME ".log");
    PerfStat::init(SAMPLE_NAME ".perf");

    jobSystem.initialize();
    PerfStat::init("import_pole.perf");

    auto graph = std::make_unique<Graph>();
    const PropertyTypeMap& propTypes = graph->getMetadata()->propTypes();
    const std::string turingHome = std::getenv("TURING_HOME");
    const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / "pole-db";

    auto t0 = Clock::now();
    Neo4jImporter::importJsonDir(jobSystem,
                                 graph.get(),
                                 db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                 db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                 {
                                     ._jsonDir = jsonDir,
                                 });
    auto t1 = Clock::now();

    std::cout << "Parsing: " << duration<Seconds>(t0, t1) << " s" << std::endl;

    const auto view = graph->view();
    const auto reader = view.read();

    std::string_view address = "33 Plover Drive";
    PropertyType addressType = propTypes.get("address (String)");
    auto it = reader.scanNodeProperties<types::String>(addressType._id).begin();

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
    NodeView node = reader.getNodeView(nodeID);
    t1 = Clock::now();
    std::cout << "Got node view in: " << duration<Microseconds>(t0, t1) << " us" << std::endl;

    const auto& nodeProperties = node.properties();
    const types::String::Primitive& addressValue =
        nodeProperties.getProperty<types::String>(addressType._id);

    std::cout << "Location has address: " << addressValue << std::endl;

    std::stringstream report;
    GraphReport::getReport(reader, report);
    std::cout << report.view() << std::endl;

    PerfStat::destroy();
    return 0;
}

