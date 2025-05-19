#include "CommitHash.h"

#include <random>

using namespace db;


static inline std::random_device _rd;
static inline std::mt19937 _generator(_rd());
static inline std::uniform_int_distribution<uint64_t> _distribution {
    0,
    std::numeric_limits<uint64_t>::max() - 1,
};

template<int i>
TemplateCommitHash<i> TemplateCommitHash<i>::create() {
    return TemplateCommitHash<i> {_distribution(_generator)};
}

template TemplateCommitHash<0> TemplateCommitHash<0>::create();
template TemplateCommitHash<1> TemplateCommitHash<1>::create();
