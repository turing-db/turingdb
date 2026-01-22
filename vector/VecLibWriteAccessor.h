#pragma once

#include <mutex>
#include <shared_mutex>

#include "VecLibMetadata.h"
#include "VectorResult.h"

namespace vec {

class VecLib;
class VectorDatabase;
class BatchVectorCreate;

class VecLibWriteAccessor {
public:
    VecLibWriteAccessor();
    explicit VecLibWriteAccessor(VecLib& vecLib);
    ~VecLibWriteAccessor();

    VecLibWriteAccessor(const VecLibWriteAccessor&) = delete;
    VecLibWriteAccessor(VecLibWriteAccessor&&) = delete;
    VecLibWriteAccessor& operator=(const VecLibWriteAccessor&) = delete;
    VecLibWriteAccessor& operator=(VecLibWriteAccessor&&) = delete;

    bool isValid() const {
        return _vecLib != nullptr;
    }

    const VecLibMetadata& metadata() const;

    void prepareCreateBatch(BatchVectorCreate* batch);
    [[nodiscard]] VectorResult<void> addEmbeddings(const BatchVectorCreate& batch);

private:
    std::unique_lock<std::shared_mutex> _lock;
    VecLib* _vecLib {nullptr};
};

}
