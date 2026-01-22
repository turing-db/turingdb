#pragma once

#include "ColumnOptVector.h"

#include "metadata/PropertyType.h"

namespace db {

using ColumnOptMask = ColumnOptVector<CustomBool>; // TODO: Change to ColumnMask::Bool_t

}
