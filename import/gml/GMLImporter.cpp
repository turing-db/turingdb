#include "GMLImporter.h"

#include <spdlog/spdlog.h>

#include "Graph.h"
#include "GMLGraphParser.h"
#include "LogUtils.h"

using namespace db;

bool GMLImporter::importFile(JobSystem& jobs, Graph* graph, const FileUtils::Path& path) {
    if (!FileUtils::exists(path)) {
        logt::FileNotFound(path);
        return false;
    }

    std::string content;
    if (!FileUtils::readContent(path, content)) {
        logt::CanNotRead(path);
        return false;
    }

    return importContent(jobs, graph, std::string_view {content});
}

bool GMLImporter::importContent(JobSystem& jobs, Graph* graph, std::string_view data) {
    GMLGraphSax sax(graph);
    GMLGraphParser parser(sax);

    sax.prepare();
    const auto res = parser.parse(data);

    if (!res) {
        spdlog::error(res.error().getMessage());
        return false;
    }

    sax.finish(jobs);

    return true;
}

