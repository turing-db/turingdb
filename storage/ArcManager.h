#pragma once

#include <atomic>
#include <list>
#include <mutex>

namespace db {

template <typename T>
class Arc;

template <typename T>
class ArcManager;

/** @brief A weak reference to an object.
 *
 * The contained object does not get deleted when the last reference is
 * released. This is done by the ArcManager.
 *
 * `WeakArc<T>` directly references the reference count
 * owned by the `Arc<T>` object.
 * */
template <typename T>
class WeakArc {
public:
    WeakArc() = default;

    ~WeakArc() {
        if (_refs) {
            _refs->fetch_sub((size_t)1);
        }
    };

    /** @brief Copy constructor to get `WeakRef<const T>`.
     *
     * Increments the reference count.
     * */
    WeakArc(const WeakArc& other)
        : _ptr(other._ptr),
          _refs(other._refs)
    {
        if (_refs) {
            _refs->fetch_add((size_t)1);
        }
    }

    /** @brief Copy constructor to get `WeakRef<const T>`.
     *
     * Increments the reference count.
     * */
    template <typename U>
    WeakArc(const WeakArc<U>& other) noexcept
        requires std::is_same_v<const U, T>
        : _ptr(other._ptr),
          _refs(other._refs)
    {
        if (_refs) {
            _refs->fetch_add((size_t)1);
        }
    }

    /** @brief Copy assignment to get `WeakRef<const T>`.
     *
     * Increments the reference count.
     * */
    WeakArc& operator=(const WeakArc& other) {
        if (this == &other) {
            // Protects against self-assignment which would
            // lead to calling sub() then add() on the same object,
            // with the possibility for the ArcManager to briefly 
            // see ref() == 0 and incorrectly delete the object.
            return *this;
        }

        // Decrement the reference count of the old object.
        if (_refs) {
            _refs->fetch_sub((size_t)1);
        }

        _ptr = other._ptr;
        _refs = other._refs;

        // Increment the reference count of the new object.
        if (_refs) {
            _refs->fetch_add((size_t)1);
        }

        return *this;
    }

    /** @brief Copy assignment to get `WeakRef<const T>`.
     * */
    template <typename U>
    WeakArc& operator=(const WeakArc<U>& other) noexcept
        requires std::is_same_v<const U, T>
    {
        if (this == &other) {
            // Protects against self-assignment which would
            // lead to calling sub() then add() on the same object,
            // with the possibility for the ArcManager to briefly 
            // see ref() == 0 and incorrectly delete the object.
            return *this;
        }

        // Decrement the reference count of the old object.
        if (_refs) {
            _refs->fetch_sub((size_t)1);
        }

        _ptr = other._ptr;
        _refs = other._refs;

        // Increment the reference count of the new object.
        if (_refs) {
            _refs->fetch_add((size_t)1);
        }

        return *this;
    }

    WeakArc(WeakArc&& other) noexcept
        : _ptr(other._ptr),
          _refs(other._refs)
    {
        other._ptr = nullptr;
        other._refs = nullptr;
    }

    WeakArc& operator=(WeakArc&& other) noexcept {
        if (this == &other) {
            return *this;
        }

        _ptr = other._ptr;
        _refs = other._refs;
        other._ptr = nullptr;
        other._refs = nullptr;

        return *this;
    }

    const T* operator->() const {
        return _ptr;
    }

    T* operator->() {
        return _ptr;
    }

    [[nodiscard]] const T* get() const {
        return _ptr;
    }

    [[nodiscard]] T* get() {
        return _ptr;
    }

    [[nodiscard]] const T& operator*() const {
        return *_ptr;
    }

    [[nodiscard]] T& operator*() {
        return *_ptr;
    }

    operator bool() const {
        return _ptr != nullptr;
    }

    bool operator==(const WeakArc<T>& other) const {
        return _ptr == other._ptr;
    }

    bool operator!=(const WeakArc<T>& other) const {
        return _ptr != other._ptr;
    }

    bool operator==(const T* other) const {
        return _ptr == other;
    }

    bool operator!=(const T* other) const {
        return _ptr != other;
    }

private:
    friend class Arc<T>;
    friend class WeakArc<const T>;

    T* _ptr {nullptr};
    mutable std::atomic<size_t>* _refs {nullptr};

    WeakArc(T* ptr, std::atomic<size_t>* refs)
        : _ptr(ptr),
          _refs(refs)
    {
        _refs->fetch_add((size_t)1);
    }
};

/** @brief A reference counted object.
 *
 * The contained object does not get deleted when the last reference is
 * released. This is done by the ArcManager.
 *
 * `Arc<T>` is the owner of the reference count.
 * */
template <typename T>
class Arc {
public:
    Arc() = default;
    ~Arc() = default;

    explicit Arc(T* ptr)
        : _ptr(ptr),
          _refs(0)
    {
    }

    Arc(const Arc& other) = delete;
    Arc& operator=(const Arc& other) = delete;

    Arc(Arc&& other) noexcept
        : _ptr(other._ptr),
          _refs(other._refs.load())
    {
        other._ptr = nullptr;
        other._refs.store(0);
    }

    Arc& operator=(Arc&& other) noexcept {
        if (this == &other) {
            return *this;
        }

        _ptr = other._ptr;
        _refs = other._refs.load();
        other._ptr = nullptr;
        other._refs.store(0);

        return *this;
    }

    const T* operator->() const {
        return _ptr;
    }

    T* operator->() {
        return _ptr;
    }

    [[nodiscard]] const T& operator*() const {
        return *_ptr;
    }

    [[nodiscard]] T& operator*() {
        return *_ptr;
    }

    [[nodiscard]] const T* get() const {
        return _ptr;
    }

    [[nodiscard]] T* get() {
        return _ptr;
    }

    operator bool() const {
        return _ptr != nullptr;
    }

    /** @brief Returns a weak reference to this object.
     * */
    [[nodiscard]] WeakArc<T> getWeak() {
        return {_ptr, &_refs};
    }

    /** @brief Returns the number of references to this object.
     * */
    [[nodiscard]] size_t refs() const {
        return _refs.load();
    }

private:
    friend class Arc<const T>;
    friend class WeakArc<T>;

    T* _ptr {nullptr};
    mutable std::atomic<size_t> _refs {0};
};


/** Manages a set of atomically reference counted objects.
 * */
template <typename T>
class ArcManager {
public:
    ArcManager() = default;

    ~ArcManager() {
        for (auto& arc : _arcs) {
            delete arc.get();
        }
    }

    ArcManager(const ArcManager&) = delete;
    ArcManager(ArcManager&&) = delete;
    ArcManager& operator=(const ArcManager&) = delete;
    ArcManager& operator=(ArcManager&&) = delete;

    /** @brief Creates a new object and returns a weak reference to it.
     *
     * @param args Arguments to pass to the constructor of T.
     *
     * @return A weak reference to the newly created object.
     *
     * @code
     *    WeakArc<MyObject> ref = arcManager.create<MyObject>(1, 2, 3);
     * @endcode
     * */
    template <typename... Args>
    [[nodiscard]] WeakArc<T> create(Args&&... args) {
        std::scoped_lock guard {_mutex};

        auto& arc = _arcs.emplace_back(new T(std::forward<Args>(args)...));

        return arc.getWeak();
    }

    /** @brief Takes ownership of an object.
     *
     * The object will be deleted when the last reference is released.
     * */
    [[nodiscard]] WeakArc<T> takeOwnership(T* ptr) {
        std::scoped_lock guard {_mutex};

        auto& arc = _arcs.emplace_back(ptr);

        return arc.getWeak();
    }

    /** @brief Cleans up all objects referenced only by the manager.
     * */
    size_t cleanUp() {
        std::scoped_lock guard {_mutex};

        size_t count = 0;

        for (auto it = _arcs.begin(); it != _arcs.end();) {
            if (it->refs() == 0) {
                count++;
                delete it->get();
                it = _arcs.erase(it);
            } else {
                ++it;
            }
        }

        return count;
    }

    [[nodiscard]] size_t size() const {
        std::scoped_lock guard {_mutex};
        return _arcs.size();
    }

private:
    mutable std::mutex _mutex;

    std::list<Arc<T>> _arcs;
};

}
