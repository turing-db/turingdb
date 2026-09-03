#include <stdio.h>
#include <stdlib.h>
#include <string>

#include "ColumnConst.h"
#include "ColumnContainer.h"
#include "ColumnVector.h"

using namespace wasm;

int main() {
    auto* ids = new ColumnVector<uint64_t>(ColumnType::NODE_ID);
    ids->resize(4);
    ids->data()[2] = 7;

    auto* strings = new ColumnVector<std::string>(ColumnType::STRING);
    strings->reserve(2);
    strings->emplace_back("abc", 3);
    strings->emplace_back(size_t(5), '\0');

    auto* optionalIds = new ColumnVector<std::optional<uint64_t>>(ColumnType::UINT64);
    optionalIds->resize(3);
    (*optionalIds)[1] = 9;

    auto* name = new ColumnConst<std::string>(ColumnType::STRING);
    name->set(std::string("turing"));

    auto* optionalScore = new ColumnConst<std::optional<double>>(ColumnType::DOUBLE);
    optionalScore->set(1.5);

    ColumnContainer container;
    container.addColumn(ids, "ids");
    container.addColumn(strings, "strings");
    container.addColumn(optionalIds, "optionalIds");
    container.addColumn(name, "name");
    container.addColumn(optionalScore, "optionalScore");

    const bool kindsOk = container[0]->getKind() == ColumnKind::VECTOR
                         && container[2]->getKind() == ColumnKind::OPTIONAL_VECTOR
                         && container[3]->getKind() == ColumnKind::CONSTANT
                         && container[4]->getKind() == ColumnKind::OPTIONAL_CONSTANT;

    const bool valuesOk = (*ids)[2] == 7
                          && strings->size() == 2
                          && (*optionalIds)[1].has_value()
                          && name->getValue() == "turing"
                          && optionalScore->getValue().has_value()
                          && container.getName(1) == "strings";

    if (!kindsOk || !valuesOk) {
        printf("wasm column smoke test FAILED\n");
        return EXIT_FAILURE;
    }

    printf("wasm column smoke test passed: %zu columns\n", container.size());
    return EXIT_SUCCESS;
}
