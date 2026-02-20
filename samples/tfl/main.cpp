#include <stdlib.h>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>
#include <spdlog/fmt/bundled/format.h>

#include "TuringDB.h"
#include "TuringConfig.h"
#include "SystemManager.h"
#include "Graph.h"
#include "LocalMemory.h"
#include "dataframe/Dataframe.h"
#include "columns/ColumnVector.h"

#include "ToolInit.h"

using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("tfl");
    toolInit.disableOutputDir();
    toolInit.init(argc, argv);

    fs::Path turingDir = fs::Path(SAMPLE_DIR) / ".turing";
    if (turingDir.exists()) {
        turingDir.rm();
    }

    TuringConfig config;
    config.setTuringDirectory(turingDir);
    config.setSyncedOnDisk(false);

    TuringDB db(&config);
    LocalMemory mem;
    db.init();

    const std::string graphName = "tfl";
    db.getSystemManager().createGraph(graphName);

    fs::Path csvPath = fs::Path(SAMPLE_DIR) / "tfl" / "stations.csv";

    const std::string query =
        "LOAD CSV '" + std::string(csvPath.get()) + "' WITH HEADERS AS row "
        "RETURN row.NAME AS name, row.LINES AS lines";

    std::vector<std::string> names;
    std::vector<std::string> lines;

    const auto status = db.query(query, graphName, &mem,
        [&](const Dataframe* df) {
            using StringCol = ColumnVector<std::string>;

            auto* nameCol = df->cols()[0]->as<StringCol>();
            auto* linesCol = df->cols()[1]->as<StringCol>();

            for (size_t i = 0; i < nameCol->size(); i++) {
                names.emplace_back((*nameCol)[i]);
                lines.emplace_back((*linesCol)[i]);
            }
        });

    if (!status.isOk()) {
        spdlog::error("Query failed: {}", status.getError());
        return EXIT_FAILURE;
    }

    // Find column widths
    size_t nameWidth = 7; // "Station"
    size_t linesWidth = 5; // "Lines"
    for (size_t i = 0; i < names.size(); i++) {
        nameWidth = std::max(nameWidth, names[i].size());
        linesWidth = std::max(linesWidth, lines[i].size());
    }

    // Print table
    fmt::print("{:<{}}  {:<{}}\n", "Station", nameWidth,
                                   "Lines", linesWidth);
    fmt::print("{:-<{}}  {:-<{}}\n", "", nameWidth, "", linesWidth);
    for (size_t i = 0; i < names.size(); i++) {
        fmt::print("{:<{}}  {:<{}}\n", names[i], nameWidth,
                                       lines[i], linesWidth);
    }

    fmt::print("\n{} stations\n", names.size());

    return EXIT_SUCCESS;
}
