#include <memory>
#include <ranges>
#include <iostream>

#include <spdlog/spdlog.h>

#include "LocalMemory.h"
#include "sort.h"
#include "utils.h"

#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"

using namespace db;

template<std::ranges::forward_range Rg>
void print_range(Rg&& range, std::string_view name) {
    std::cout << name << ": ";
    for (auto&& x : range) {
        std::cout << x << ' ';
    } std::cout << '\n';
}

int main() {
    LocalMemory mem;
    DataframeManager dfman;
    auto df = makeDataframe<Int>(mem,
        dfman, {
                   {2, 4, 1, 4, 6},
                   {1, 9, 3, 4, 2},
                   {8, 6, 9, 7, 5},
                   {2, 4, 1, 3, 5}
    });

    spdlog::info("Pre sort:");
    df->dump(std::cout);

    subsort(df.get());

    fmt::print("\n\n\n");

    spdlog::info("Post sort:");
    df->dump(std::cout);
}
