#pragma once

#include <stddef.h>

namespace db {

// AddressSanitizer surrounds stack variables with redzones: the deepest query the parser
// accepts takes 880 KB of stack under it, against at most 384 KB without
#if defined(__SANITIZE_ADDRESS__)
inline constexpr size_t STACK_SCALE = 3;
#else
inline constexpr size_t STACK_SCALE = 1;
#endif

}
