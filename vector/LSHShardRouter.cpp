#include "LSHShardRouter.h"

#include "BioAssert.h"
#include "RandomGenerator.h"

using namespace vec;

LSHShardRouter::LSHShardRouter(size_t dim, uint8_t nbits) :
    _dim(dim),
    _nbits(nbits)
{
}

LSHShardRouter::~LSHShardRouter() {
}

void LSHShardRouter::initialize() {
    msgbioassert(_dim > 0, "LSHShardRouter: dimension must be greater than 0");
    msgbioassert(_nbits >= 2 && _nbits < 16, "LSHShardRouter: number of bits must be between 2 and 15");
    _hyperplanes.resize(_nbits);

    for (auto& hyperplane : _hyperplanes) {
        hyperplane.resize(_dim);

        for (float& value : hyperplane) {
            value = RandomGenerator::generate<float>() - 0.5;
        }
    }
}

LSHSignature LSHShardRouter::getSignature(std::span<const float> vector) const {
    LSHSignature signature = 0;
    float dot = 0.0f;
    const float* hyperplane = nullptr;

    for (size_t i = 0; i < _nbits; i++) {
        dot = 0.0;
        hyperplane = _hyperplanes[i].data();

        for (size_t j = 0; j < _dim; j++) {
            dot += vector[j] * hyperplane[j];
        }

        if (dot > 0.0f) {
            signature |= (1ull << i);
        }
    }

    return signature;
}
