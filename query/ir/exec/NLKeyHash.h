#pragma once

#include <stdint.h>

namespace db {

// The 64-bit finalizer of MurmurHash3, so a key's bits spread over every bucket. Inline,
// since the joins and the group tables call it once per row of their hot loops.
inline uint64_t mixKeyBits(uint64_t bits) {
    bits ^= bits >> 33;
    bits *= 0xff51afd7ed558ccdULL;
    bits ^= bits >> 33;
    bits *= 0xc4ceb9fe1a85ec53ULL;
    bits ^= bits >> 33;

    return bits;
}

}
