#include "FaissIdx.h"

#include <faiss/Index.h>

static_assert(std::is_same_v<faiss::idx_t, int64_t>);

