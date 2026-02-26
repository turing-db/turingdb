#include "VecLibAccessor.h"

#include "VecLib.h"
#include "VectorSearchQuery.h"
#include "VectorSearchResult.h"

#include "BioAssert.h"

using namespace vec;

VecLibAccessor::VecLibAccessor()
{
}

VecLibAccessor::VecLibAccessor(VecLib* vecLib)
    : _lock(vecLib->_mutex),
    _vecLib(vecLib)
{
}

VecLibAccessor::~VecLibAccessor() {
}

const VecLibMetadata* VecLibAccessor::metadata() const {
    bioassert(_vecLib, "Invalid VecLib accessor");
    return _vecLib->metadata();
}

void VecLibAccessor::prepareCreateBatch(BatchVectorCreate* batch) {
    bioassert(_vecLib, "Invalid VecLib accessor");
    _vecLib->prepareCreateBatch(batch);
}

VectorResult<void> VecLibAccessor::addEmbeddings(const BatchVectorCreate* batch) {
    bioassert(_vecLib, "Invalid VecLib accessor");
    return _vecLib->addEmbeddings(batch);
}


VectorResult<void> VecLibAccessor::search(const VectorSearchQuery* query,
                                          VectorSearchResult* results) {
    bioassert(_vecLib, "Invalid VecLib accessor");
    return _vecLib->search(query, results);
}

