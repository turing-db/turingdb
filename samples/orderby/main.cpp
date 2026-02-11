#include <memory>
#include <ranges>
#include <iostream>

#include <spdlog/spdlog.h>

#include "sort.h"

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
    DataframeManager dfman;
    auto df = std::make_unique<Dataframe>();

    std::vector<ColumnInts> cols(4);
    {
        cols[0] = {2, 4, 1, 4, 6};
        cols[1] = {1, 9, 3, 4, 2};
        cols[2] = {8, 6, 9, 7, 5};
        cols[3] = {2, 4, 1, 3, 5};

        for (auto&& col : cols) {
            const ColumnTag t = dfman.allocTag();
            NamedColumn* ncol = NamedColumn::create(&dfman, &col, t);
            df->addColumn(ncol);
        }
    }

    spdlog::info("Pre sort:");
    df->dump(std::cout);

    sort(df.get());

    fmt::print("\n\n\n");

    spdlog::info("Post sort:");
    df->dump(std::cout);
}
