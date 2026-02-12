#pragma once

#include <concepts>
#include <string_view>

namespace db {

class Dataframe;

template <typename T>
concept Writer = requires(T writer) {
    { writer.write(std::string_view {}) } noexcept -> std::same_as<void>;
};

}
