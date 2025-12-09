#include "RandomGenerator.h"

#include <random>

#include "VectorException.h"

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

uint64_t RandomGenerator::generate() {
    if (!_impl) {
        throw VectorException("Random generator not initialized");
    }

    return _impl->_generator();
}

}
