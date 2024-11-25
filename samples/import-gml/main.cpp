#include <spdlog/fmt/bundled/core.h>

#include "DB.h"
#include "DBReader.h"
#include "GMLImporter.h"
#include "JobSystem.h"
#include "LogSetup.h"
#include "PerfStat.h"

using namespace db;

int main() {
    JobSystem jobSystem;
    LogSetup::setupLogFileBacked(SAMPLE_NAME ".log");
    PerfStat::init(SAMPLE_NAME ".perf");

    jobSystem.initialize();
    PerfStat::init("import_pole.perf");


    {
        FileUtils::Path path = SAMPLE_DIR "/simple_graph.gml";
        auto database = std::make_unique<DB>();
        GMLImporter importer;

        fmt::print("Importing {}\n", path.c_str());
        if (!importer.importFile(jobSystem, database.get(), path)) {
            return 1;
        }

        const auto rd = database->read();
        fmt::print("Node count: {}\n", rd.getNodeCount());
        fmt::print("Edge count: {}\n", rd.getEdgeCount());

        for (auto label : rd.scanNodeProperties<types::String>(0)) {
            fmt::print("[Node \"{}\"]\n", label);
        }
        auto range = rd.scanEdgeProperties<types::String>(0);
        auto it = range.begin();
        for (; it.isValid(); it.next()) {
            const auto id = it.getCurrentEdgeID();
            const auto* edge = rd.getEdge(id);
            const auto src = rd.getNodeView(edge->_nodeID);
            const auto tgt = rd.getNodeView(edge->_otherID);
            const std::string& srcName = src.properties().getProperty<types::String>(0);
            const std::string& tgtName = tgt.properties().getProperty<types::String>(0);
            fmt::print("[Edge \"{}\" from={} to={}]\n", it.get(), srcName, tgtName);
        }
    }

    return 0;
}

