#include "RandomGenerator.h"

#include <random>

#include "VectorException.h"
#include "Panic.h"

namespace vec {

struct RandomGenerator::Impl {
    std::random_device _rd;
    std::mt19937_64 _generator {_rd()};
};

static void deleteImpl(RandomGenerator::Impl* impl) {
    delete impl;
}

std::unique_ptr<RandomGenerator::Impl, void (*)(RandomGenerator::Impl*)> RandomGenerator::_impl {nullptr, deleteImpl};

void RandomGenerator::initialize() {
    if (_impl) {
        return;
    }

    _impl = {new Impl, deleteImpl};
}

template <typename T>
T RandomGenerator::generate() {
    if (!_impl) {
        throw VectorException("Random generator not initialized");
    }

    if constexpr (std::is_integral_v<T>) {
        return (T)_impl->_generator();
    } else if constexpr (std::is_floating_point_v<T>) {
        return (T)_impl->_generator() / (T)std::numeric_limits<T>::max();
    } else {
        COMPILE_ERROR("RandomGenerator: unsupported type");
    }
}

template int32_t RandomGenerator::generate<int32_t>();
template uint32_t RandomGenerator::generate<uint32_t>();
template int64_t RandomGenerator::generate<int64_t>();
template uint64_t RandomGenerator::generate<uint64_t>();
template float RandomGenerator::generate<float>();
template double RandomGenerator::generate<double>();

}
