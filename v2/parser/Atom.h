#pragma once

#include <variant>

#include "Literal.h"
#include "Parameter.h"
#include "Symbol.h"

namespace db {

template <typename T>
concept IsAtom = std::is_same_v<T, Symbol>
              || std::is_same_v<T, Literal>
              || std::is_same_v<T, Parameter>;

using Atom = std::variant<Symbol, Literal, Parameter>;

}
