#pragma once

#include "CellTag.h"

namespace db {

[[noreturn]] void throwIndexTypeError(CellTag base, CellTag index);

}
