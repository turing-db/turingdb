#pragma once

#include <string_view>

#include "MapView.h"
#include "MapEntryView.h"

namespace db {

// Entries are meant to be key-sorted, but MapBuffer::insert does not sort - only the two
// producers feeding it do - so a scan is the reading that holds for every producer. Maps
// are small enough that it costs nothing; revisit if that stops being true.
inline bool findMapEntry(const MapView map, const std::string_view key, MapEntryView& entry) {
    for (const MapEntryView candidate : map) {
        if (candidate.getKey() == key) {
            entry = candidate;
            return true;
        }
    }

    return false;
}

}
