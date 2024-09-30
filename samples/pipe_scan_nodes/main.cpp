#include <stdlib.h>

#include "PipeUtils.h"

#include <spdlog/spdlog.h>

int main() {
    PipeSample sample("pipe_scan_nodes");

    if (!sample.loadJsonDB(sample.getTuringHome() + "/neo4j/pole-db/")) {
        return EXIT_FAILURE;
    }

    if (!sample.executeQuery("SELECT * FROM n:")) {
        spdlog::error("Failed to execute query");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}