#include <cstdio>

#include "columns/ColumnConst.h"
#include "columns/ColumnSwitch.h"
#include "spdlog/spdlog.h"

using namespace db;

int main() {
    puts("0");

    Column* col = new ColumnVector<NodeID>({0, 1, 2, 3});
    Column* colConst = new ColumnConst<NodeID>(0);

    dispatchColumnVector(col,
                   [](auto* c) { spdlog::info("Vector is empty ? {}", c->empty()); });

    dispatchColumnVector(colConst,
                   [](auto* c) { spdlog::info("Vector is empty ? {}", c->empty()); });
}
