#pragma once

#include <functional>
#include <memory>
#include <optional>

namespace vec {

class RandomGenerator {
public:
    struct Impl;
    using ImplDeleter = void (*)(Impl*);
    using ImplUniquePtr = std::unique_ptr<Impl, ImplDeleter>;

    ~RandomGenerator() = delete;
    RandomGenerator() = delete;
    RandomGenerator(const RandomGenerator&) = delete;
    RandomGenerator& operator=(const RandomGenerator&) = delete;
    RandomGenerator(RandomGenerator&&) = delete;
    RandomGenerator& operator=(RandomGenerator&&) = delete;

    static void initialize(std::optional<uint64_t> seed = {});

    [[nodiscard]] static bool initialized() {
        return _impl != nullptr;
    }

    template <typename T = uint64_t>
    [[nodiscard]] static T generate();

    template <typename T = uint64_t>
    [[nodiscard]] static T generateUnique(const std::function<bool(T)>& predicate);

private:
    static ImplUniquePtr _impl;
};

}
