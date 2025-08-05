#include "indexes/StringIndex.h"

namespace db {

class StringIndexDumpConstants {
public:
    static constexpr size_t NODESIZE =
        sizeof(char)                                                // _val
        + sizeof(uint8_t)                                           // _isComplete
        + sizeof(size_t)                                            // _owners.size()
        + ((StringIndex::ALPHABET_SIZE + 7) / 8) * sizeof(uint8_t); // Child bitmap

    // One bit for each child
    static constexpr size_t CHILDMASKSIZE = (StringIndex::ALPHABET_SIZE + 7) / 8;
};
}
