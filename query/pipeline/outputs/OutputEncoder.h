#pragma once

#include <concepts>

namespace db {

class Dataframe;

template <typename T>
concept Encoder = requires(T encoder, const Dataframe& df) {
    { encoder.writeDataframeHeader(df) } -> std::same_as<void>;
    { encoder.writeDataframe(df) } -> std::same_as<void>;
};

}
