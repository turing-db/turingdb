#pragma once

#include <shared_mutex>

#include "VectorResult.h"

namespace vec {

class VecLib;
class BatchVectorCreate;
class VectorSearchQuery;
class VectorSearchResult;

class VecLibAccessor {
public:
    VecLibAccessor();
    ~VecLibAccessor();

    VecLibAccessor(std::shared_mutex& mutex, VecLib& vecLib);

    VecLibAccessor(const VecLibAccessor&) = delete;
    VecLibAccessor(VecLibAccessor&&) = delete;
    VecLibAccessor& operator=(const VecLibAccessor&) = delete;
    VecLibAccessor& operator=(VecLibAccessor&&) = delete;

    [[nodiscard]] bool isValid() const {
        return _vecLib != nullptr;
    }

    [[nodiscard]] BatchVectorCreate prepareCreateBatch();
    [[nodiscard]] VectorResult<void> addEmbeddings(const BatchVectorCreate& batch);
    [[nodiscard]] VectorResult<void> search(const VectorSearchQuery& query, VectorSearchResult& results);

private:
    std::shared_lock<std::shared_mutex> _lock;

    VecLib* _vecLib {nullptr};
};

}
