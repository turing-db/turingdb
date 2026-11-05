#pragma once

#include <stddef.h>

namespace db {

class Column;
class ParameterValue;

void readParameterValue(const Column* column, size_t row, ParameterValue& value);

}
