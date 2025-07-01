#include "cli.h"
#include "benchmark.h"
#include "utils.h"

#include <iostream>
#include <string>

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--cli") {
        cli();
    }

    else if (argc == 3 && std::string(argv[1]) == "--ppx") {
        auto arg = std::string(argv[2]);
        auto tokens = preprocess(arg);
        for (const auto& t : tokens) std::cout << t << std::endl;
    }
    else if (argc == 4 && std::string(argv[1]) == "--bm") {
        auto input = std::string(argv[2]);
        auto queries = std::string(argv[3]);
        benchmarkPrefix(input, queries);
    }
}
