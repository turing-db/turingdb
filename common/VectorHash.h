#pragma once

#include <vector>
#include <functional>
#include <type_traits>

template <class T>
struct VectorHash {
    // Uses the hash of objects of type T if T is a class or POD
    // or uses the hash of the objects pointed by T if T is a pointer.
    // Use case: we support both std::vector<std::string> and std::vector<std::string*>
    std::size_t operator()(const std::vector<T>& vec) const {
        std::size_t value = vec.size();
        for (const auto& data : vec) {
            std::size_t x = 0;
            if constexpr (std::is_pointer_v<T>) {
                using DataType = typename std::remove_pointer<T>::type;
                using DataTypeWithoutConst = typename std::remove_const<DataType>::type;
                x = std::hash<DataTypeWithoutConst>{}(*data);
            } else {
                x = std::hash<T>{}(data);
            }
            x = ((x >> 16) ^ x) * 0x45d9f3b;
            x = ((x >> 16) ^ x) * 0x45d9f3b;
            x = (x >> 16) ^ x;
            value ^= x + 0x9e3779b9 + (value << 6) + (value >> 2);
        }

        return value;
    }
};