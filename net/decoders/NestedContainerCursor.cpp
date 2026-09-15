#include "NestedContainerCursor.h"

using namespace net::proto;

NestedContainerCursor::NestedContainerCursor(db::ListWriteCursor cursor)
    : _kind(NestedContainerKind::List),
    _list(cursor)
{
}

NestedContainerCursor::NestedContainerCursor(db::MapWriteCursor cursor)
    : _kind(NestedContainerKind::Map),
    _map(cursor)
{
}
