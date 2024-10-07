#include <stdlib.h>

#include <spdlog/spdlog.h>

#include "PipeUtils.h"

int main() {
    PipeSample sample("simple_server");

/*
    if (!sample.loadJsonDB(sample.getTuringHome() + "/neo4j/pole-db/")) {
        return EXIT_FAILURE;
    }
*/
    sample.createSimpleGraph();
    sample.startHttpServer();

    return EXIT_SUCCESS;
}