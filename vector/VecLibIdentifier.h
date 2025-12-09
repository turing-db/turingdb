#pragma once

#include <string_view>

namespace vec {

class RandomGenerator;

using VecLibID = std::string_view;

class VecLibIdentifier {
public:
    static constexpr size_t MAX_BASENAME_SIZE = 64;
    static constexpr size_t RAND_ID_SIZE = 16;

    VecLibIdentifier() = default;

    ~VecLibIdentifier()
    {
        delete[] _data;
    }

    VecLibIdentifier(VecLibIdentifier&& other) noexcept
        : _data(other._data),
        _size(other._size)
    {
        other._data = nullptr;
        other._size = 0;
    }

    VecLibIdentifier& operator=(VecLibIdentifier&& other) noexcept {
        if (this == &other) {
            return *this;
        }

        _data = other._data;
        _size = other._size;
        other._data = nullptr;
        other._size = 0;

        return *this;
    }

    VecLibIdentifier(const VecLibIdentifier& ) = delete;
    VecLibIdentifier& operator=(const VecLibIdentifier&) = delete;

    [[nodiscard]] static VecLibIdentifier alloc(std::string_view baseName);

    [[nodiscard]] std::string_view view() const {
        return {_data, _size};
    }

    [[nodiscard]] bool valid() const {
        return _data != nullptr;
    }

    bool operator==(const VecLibIdentifier& other) const {
        return view() == other.view();
    }

    bool operator!=(const VecLibIdentifier& other) const {
        return !(*this == other);
    }

    struct Equal {
        bool operator()(const VecLibIdentifier& lhs, const VecLibIdentifier& rhs) const {
            return lhs.view() == rhs.view();
        }
    };

    struct Hash {
        size_t operator()(const VecLibIdentifier& id) const {
            return std::hash<std::string_view>{}(id.view());
        }
    };

    operator VecLibID() const {
        return view();
    }

private:
    char* _data {nullptr};
    size_t _size {0};
};

}
