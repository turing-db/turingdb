#include <stdlib.h>

#include <iostream>

#include "PipeUtils.h"

#include <spdlog/spdlog.h>

int main() {
    PipeSample sample("pipe_scan_nodes");

/*
    if (!sample.loadJsonDB(sample.getTuringHome() + "/neo4j/pole-db/")) {
        return EXIT_FAILURE;
    }
*/
    sample.createSimpleGraph();

    std::cout << "Persons:\n";
    if (!sample.executeQuery("SELECT * FROM n:Person")) {
        spdlog::error("Failed to execute query");
        return EXIT_FAILURE;
    }

    std::cout << "\nFounders:\n";
    if (!sample.executeQuery("SELECT * FROM f:Founder")) {
        spdlog::error("Failed to execute query");
        return EXIT_FAILURE;
    }

    std::cout << "\nBioinformatics:\n";
    if (!sample.executeQuery("SELECT * FROM b:Bioinformatics")) {
        spdlog::error("Failed to execute query");
        return EXIT_FAILURE;
    }

    std::cout << "\nBioinformatics & Founder:\n";
    if (!sample.executeQuery("SELECT * FROM bf:Bioinformatics,Founder")) {
        spdlog::error("Failed to execute query");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}