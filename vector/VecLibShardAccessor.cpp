#include "VecLibShardAccessor.h"

#include "VecLibShard.h"
#include "AgingRingCache.h"

using namespace vec;

VecLibShardAccessor::VecLibShardAccessor(Handle&& handle)
    : _lock(handle->get()->_mutex),
    _handle(std::move(handle))
{
}

VecLibShardAccessor::~VecLibShardAccessor() {
}
