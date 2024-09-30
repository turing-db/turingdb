#include <stdlib.h>

#include "PipeUtils.h"

int main() {
    PipeSample sample("pipe_scan_nodes");

    if (!sample.loadJsonDB(sample.getTuringHome() + "/neo4j/pole-db/")) {
        return EXIT_FAILURE;
    }

    if (!sample.executeQuery("SELECT * FROM n:")) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}