#pragma once

#include <span>
#include <stack>

#include "BioAssert.h"
#include "NestedContainerKind.h"
#include "list/ListElementView.h"
#include "map/MapEntryView.h"

namespace net::proto {

/**
 * @brief One level of a nested value being written: the iterator pair the drain loop resumes
 * from. The writer's stack is a stack of these, as the decoder's is a stack of
 * @ref NestedContainerCursor.
 *
 * Both iterator pairs are held whatever the kind, so the constructors are private and each
 * sets the kind from the span type it takes: a level can only be built through @ref list or
 * @ref map, and its kind can never disagree with what it was built from. Reading the current
 * value goes through @ref getElement / @ref getEntry, which hold callers to the kind.
 */
class NestedContainerIterator {
public:
    using Stack = std::stack<NestedContainerIterator>;

    static NestedContainerIterator list(std::span<const db::ListElementView> elements) { return NestedContainerIterator {elements}; }
    static NestedContainerIterator map(std::span<const db::MapEntryView> entries) { return NestedContainerIterator {entries}; }

    bool isMap() const { return _kind == NestedContainerKind::Map; }
    bool isExhausted() const { return isMap() ? _mapIt == _mapEnd : _listIt == _listEnd; }

    void advance() {
        if (isMap()) {
            ++_mapIt;
        } else {
            ++_listIt;
        }
    }

    db::ListElementView getElement() const {
        bioassert(_kind == NestedContainerKind::List, "Nested container is a map, not a list");
        return *_listIt;
    }

    db::MapEntryView getEntry() const {
        bioassert(_kind == NestedContainerKind::Map, "Nested container is a list, not a map");
        return *_mapIt;
    }

private:
    NestedContainerKind _kind {NestedContainerKind::List};
    std::span<const db::ListElementView>::iterator _listIt {};
    std::span<const db::ListElementView>::iterator _listEnd {};
    std::span<const db::MapEntryView>::iterator _mapIt {};
    std::span<const db::MapEntryView>::iterator _mapEnd {};

    explicit NestedContainerIterator(std::span<const db::ListElementView> elements);
    explicit NestedContainerIterator(std::span<const db::MapEntryView> entries);
};

}
