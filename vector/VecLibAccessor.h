#pragma once

#include <shared_mutex>

#include "VecLibMetadata.h"
#include "VectorResult.h"

namespace vec {

class VecLib;
class BatchVectorCreate;
class VectorSearchQuery;
class VectorSearchResult;

class VecLibAccessor {
public:
    VecLibAccessor();
    explicit VecLibAccessor(VecLib* vecLib);

    ~VecLibAccessor();

    VecLibAccessor(const VecLibAccessor&) = delete;
    VecLibAccessor(VecLibAccessor&&) = delete;
    VecLibAccessor& operator=(const VecLibAccessor&) = delete;
    VecLibAccessor& operator=(VecLibAccessor&&) = delete;

    [[nodiscard]] bool isValid() const { return _vecLib != nullptr; }

    [[nodiscard]] const VecLibMetadata* metadata() const;

    void prepareCreateBatch(BatchVectorCreate* batch);
    [[nodiscard]] VectorResult<void> addEmbeddings(const BatchVectorCreate* batch);
    [[nodiscard]] VectorResult<void> search(const VectorSearchQuery* query, VectorSearchResult* results);

private:
    std::shared_lock<std::shared_mutex> _lock;
    VecLib* _vecLib {nullptr};
};

}
