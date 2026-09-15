#include "NestedContainerIterator.h"

using namespace net::proto;

NestedContainerIterator::NestedContainerIterator(std::span<const db::ListElementView> elements)
    : _kind(NestedContainerKind::List),
    _listIt(elements.begin()),
    _listEnd(elements.end())
{
}

NestedContainerIterator::NestedContainerIterator(std::span<const db::MapEntryView> entries)
    : _kind(NestedContainerKind::Map),
    _mapIt(entries.begin()),
    _mapEnd(entries.end())
{
}
