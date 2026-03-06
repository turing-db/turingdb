#include "VecLibShardAccessor.h"

#include "VecLibShard.h"

using namespace vec;

VecLibShardAccessor::VecLibShardAccessor(VecLibShard* shard)
    : _shard(shard),
    _lock(shard->_mutex)
{
}

VecLibShardAccessor::~VecLibShardAccessor() {
}
