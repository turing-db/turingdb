#include "CommitHash.h"

#include <random>

#include "ChangeID.h"
#include "GraphID.h"

using namespace db;

thread_local std::random_device _rd;
thread_local std::mt19937_64 _generator(_rd());
thread_local std::uniform_int_distribution<uint64_t> _distribution {
    1,
    std::numeric_limits<uint64_t>::max() - 1,
};

template <int i, int radix>
TemplateCommitHash<i, radix> TemplateCommitHash<i, radix>::create() {
    return TemplateCommitHash<i, radix> {_distribution(_generator)};
}

template CommitHash CommitHash::create();
template ChangeID ChangeID::create();
template GraphID GraphID::create();
