#pragma once

#include "FileUtils.h"

namespace db {

class JobSystem;
class Graph;

class GMLImporter {
public:
    [[nodiscard]] bool importFile(JobSystem& jobs, Graph* graph, const FileUtils::Path& path);
    [[nodiscard]] bool importContent(JobSystem& jobs, Graph* graph, std::string_view data);
};

}

