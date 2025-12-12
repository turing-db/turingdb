#include "VecLibAccessor.h"

#include "VecLib.h"
#include "BatchVectorCreate.h"
#include "VectorSearchQuery.h"
#include "VectorSearchResult.h"

#include "BioAssert.h"

using namespace vec;

VecLibAccessor::VecLibAccessor()
{
}

VecLibAccessor::~VecLibAccessor() {
}

VecLibAccessor::VecLibAccessor(std::shared_mutex& mutex, VecLib& vecLib)
    : _lock(mutex),
    _vecLib(&vecLib)
{
}

BatchVectorCreate VecLibAccessor::prepareCreateBatch() {
    bioassert(_vecLib, "Invalid VecLib accessor");
    return _vecLib->prepareCreateBatch();
}

VectorResult<void> VecLibAccessor::addEmbeddings(const BatchVectorCreate& batch) {
    bioassert(_vecLib, "Invalid VecLib accessor");
    return _vecLib->addEmbeddings(batch);
}

VectorResult<void> VecLibAccessor::search(const VectorSearchQuery& query, VectorSearchResult& results) {
    bioassert(_vecLib, "Invalid VecLib accessor");
    return _vecLib->search(query, results);
}
