#pragma once

#include <stdint.h>
#include <deque>

#include "BioAssert.h"
#include "NestedContainerKind.h"
#include "list/ListWriteCursor.h"
#include "map/MapWriteCursor.h"

namespace net::proto {

/**
 * @brief The write cursor for one container a streaming decoder has open, whose kind is only
 * known at runtime. The decoder's nesting stack is a stack of these, as the writer's is a
 * stack of @ref NestedContainerStackElement.
 *
 * Both cursors are held whatever the kind, so the constructors are private and each sets the
 * kind from the cursor type it takes: a cursor can only be built through @ref list or @ref map,
 * and its kind can never disagree with what it was built from. Reading one back out goes
 * through @ref getList / @ref getMap, which hold callers to the kind.
 */
class NestedContainerCursor {
public:
    using Stack = std::deque<NestedContainerCursor>;

    static NestedContainerCursor list(db::ListWriteCursor cursor) { return NestedContainerCursor {cursor}; }
    static NestedContainerCursor map(db::MapWriteCursor cursor) { return NestedContainerCursor {cursor}; }

    bool isMap() const { return _kind == NestedContainerKind::Map; }
    uint64_t getWritten() const { return isMap() ? _map.getWritten() : _list.getWritten(); }
    bool isComplete() const { return isMap() ? _map.isComplete() : _list.isComplete(); }

    db::ListWriteCursor& getList() {
        bioassert(_kind == NestedContainerKind::List, "Open container is a map, not a list");
        return _list;
    }

    const db::ListWriteCursor& getList() const {
        bioassert(_kind == NestedContainerKind::List, "Open container is a map, not a list");
        return _list;
    }

    db::MapWriteCursor& getMap() {
        bioassert(_kind == NestedContainerKind::Map, "Open container is a list, not a map");
        return _map;
    }

    const db::MapWriteCursor& getMap() const {
        bioassert(_kind == NestedContainerKind::Map, "Open container is a list, not a map");
        return _map;
    }

private:
    NestedContainerKind _kind {NestedContainerKind::List};
    db::ListWriteCursor _list;
    db::MapWriteCursor _map;

    explicit NestedContainerCursor(db::ListWriteCursor cursor);
    explicit NestedContainerCursor(db::MapWriteCursor cursor);
};

}
