#include <cstdio>

#include "columns/ColumnSwitch.h"

using namespace db;

#define CASE_COMPONENT(col, Type)                      \
    { auto* c = static_cast<Type*>(col); }

int main() {
    puts("0");

    Column* col = new ColumnVector<NodeID>({0, 1, 2, 3});

    COLUMN_SWITCH(col);
}
