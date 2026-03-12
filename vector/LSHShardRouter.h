#pragma once

#include <set>
#include <span>
#include <vector>
#include <stdint.h>

#include "LSHSignature.h"

namespace vec {

class LSHShardRouter {
public:
    LSHShardRouter() = delete;
    ~LSHShardRouter();

    LSHShardRouter(size_t dim, uint8_t nbits);

    LSHShardRouter(const LSHShardRouter&) = delete;
    LSHShardRouter(LSHShardRouter&&) = delete;
    LSHShardRouter& operator=(const LSHShardRouter&) = delete;
    LSHShardRouter& operator=(LSHShardRouter&&) = delete;

    void initialize();

    void registerShardSignature(LSHSignature signature);

    LSHSignature getSignature(std::span<const float> vector) const;

    void getSearchSignatures(std::span<const float> vector,
                             std::vector<LSHSignature>& signatures) const;

    const std::set<LSHSignature>& getInstantiatedShardSignatures() const {
        return _instantiatedShardSignatures;
    }

private:
    friend class LSHShardRouterLoader;
    friend class LSHShardRouterWriter;

    size_t _dim {0};
    uint8_t _nbits {0};
    std::vector<std::vector<float>> _hyperplanes;
    std::set<LSHSignature> _instantiatedShardSignatures;
};

}
