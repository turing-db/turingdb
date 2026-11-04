#pragma once

#include <deque>
#include <utility>

#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/MapVector.h"

#include "BioAssert.h"

namespace db {

// An llvm::MapVector whose entries keep their address when another is inserted, which a
// vector's growth breaks, and whose at() throws on a missing key, where llvm::MapVector's
// only asserts.
template <typename Key, typename Value>
class InsertionOrderedMap : public llvm::MapVector<Key, Value, llvm::DenseMap<Key, unsigned>, std::deque<std::pair<Key, Value>>> {
public:
    Value& at(const Key& key) {
        const auto entry = this->find(key);
        bioassert(entry != this->end(), "No entry under the key read");

        return entry->second;
    }

    const Value& at(const Key& key) const {
        const auto entry = this->find(key);
        bioassert(entry != this->end(), "No entry under the key read");

        return entry->second;
    }
};

}
