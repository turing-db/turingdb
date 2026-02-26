#include "VecLibWriteAccessor.h"

#include "VecLib.h"
#include "VectorDatabase.h"
#include "BatchVectorCreate.h"

#include "BioAssert.h"

using namespace vec;

VecLibWriteAccessor::VecLibWriteAccessor()
{
}

VecLibWriteAccessor::VecLibWriteAccessor(VecLib& vecLib)
    : _lock(vecLib._mutex),
    _vecLib(&vecLib)
{
}

VecLibWriteAccessor::~VecLibWriteAccessor() {
}

const VecLibMetadata* VecLibWriteAccessor::metadata() const {
    bioassert(_vecLib, "Invalid VecLib write accessor");
    return _vecLib->metadata();
}

void VecLibWriteAccessor::prepareCreateBatch(BatchVectorCreate* batch) {
    bioassert(_vecLib, "Invalid VecLib write accessor");
    _vecLib->prepareCreateBatch(batch);
}

VectorResult<void> VecLibWriteAccessor::addEmbeddings(const BatchVectorCreate* batch) {
    bioassert(_vecLib, "Invalid VecLib write accessor");
    return _vecLib->addEmbeddings(batch);
}
