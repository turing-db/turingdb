#pragma once

#include <future>

namespace js {

template <typename T>
class TypedPromise;

class Promise {
public:
    Promise() = default;
    Promise(const Promise&) = default;
    Promise(Promise&&) = delete;
    Promise& operator=(const Promise&) = default;
    Promise& operator=(Promise&&) = delete;
    virtual ~Promise() = default;

    template <typename T>
    constexpr TypedPromise<T>* cast() {
        return static_cast<TypedPromise<T>*>(this);
    }

    virtual void finish() = 0;
};

template <typename T>
class TypedPromise : public Promise, public std::promise<T> {
public:
    void finish() override {
        if constexpr (std::is_same_v<T, void>) {
            this->set_value();
        }
    }
};

}
