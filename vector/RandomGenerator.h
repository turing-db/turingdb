#pragma once

#include <memory>

namespace vec {

class RandomGenerator {
public:
    struct Impl;

    ~RandomGenerator() = delete;
    RandomGenerator() = delete;
    RandomGenerator(const RandomGenerator&) = delete;
    RandomGenerator& operator=(const RandomGenerator&) = delete;
    RandomGenerator(RandomGenerator&&) = delete;
    RandomGenerator& operator=(RandomGenerator&&) = delete;

    static void initialize();

    template <typename T = uint64_t>
    static T generate();

private:
    static std::unique_ptr<Impl, void (*)(Impl*)> _impl;
};

}
