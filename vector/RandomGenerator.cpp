#include "RandomGenerator.h"

#include <random>

#include "BioAssert.h"
#include "VectorException.h"
#include "Panic.h"

namespace vec {

struct RandomGenerator::Impl {
    std::random_device _rd;
    std::mt19937_64 _generator;

    explicit Impl(std::optional<uint64_t> seed = {})
        : _generator(seed ? *seed : _rd())
    {
    }
};

static void deleteImpl(RandomGenerator::Impl* impl) {
    delete impl;
}

RandomGenerator::ImplUniquePtr RandomGenerator::_impl {nullptr, deleteImpl};

void RandomGenerator::initialize(std::optional<uint64_t> seed) {
    if (_impl) {
        return;
    }

    _impl = ImplUniquePtr {new Impl(seed), deleteImpl};
}

template <typename T>
T RandomGenerator::generate() {
    bioassert(_impl, "Random generator not initialized");

    if constexpr (std::is_integral_v<T>) {
        return (T)_impl->_generator();
    } else if constexpr (std::is_floating_point_v<T>) {
        return (T)_impl->_generator() / (T)std::numeric_limits<uint64_t>::max();
    } else {
        COMPILE_ERROR("RandomGenerator: unsupported type");
    }
}

template <typename T>
T RandomGenerator::generateUnique(const std::function<bool(T)>& predicate) {
    bioassert(_impl, "Random generator not initialized");

    size_t attempts = 0;
    T value {};

    do {
        value = generate<T>();
        if (attempts++ > 20) {
            throw VectorException("Could not generate unique value");
        }
    } while (predicate(value));

    return value;
}

template int32_t RandomGenerator::generate<int32_t>();
template uint32_t RandomGenerator::generate<uint32_t>();
template int64_t RandomGenerator::generate<int64_t>();
template uint64_t RandomGenerator::generate<uint64_t>();
template float RandomGenerator::generate<float>();
template double RandomGenerator::generate<double>();

template int32_t RandomGenerator::generateUnique<int32_t>(const std::function<bool(int32_t)>& predicate);
template uint32_t RandomGenerator::generateUnique<uint32_t>(const std::function<bool(uint32_t)>& predicate);
template int64_t RandomGenerator::generateUnique<int64_t>(const std::function<bool(int64_t)>& predicate);
template uint64_t RandomGenerator::generateUnique<uint64_t>(const std::function<bool(uint64_t)>& predicate);

}
