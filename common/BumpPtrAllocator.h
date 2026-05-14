#pragma once

#include <new>
#include <type_traits>
#include <stddef.h>
#include <functional>

// Bump pointer allocator
// It allocates a given number of bytes from flat buffers of bytes
// that are called slabs. The size of slabs has to be set according
// to application-specific information. The goal is to improve cache
// locality of allocated objects and to minimize the calls to malloc.
//
// Because of slabs it is not a strict bump pointer allocator but
// some kind of piecewise bump allocator if you wish.
//
// You can create objects and they are all destroyed together when
// the allocator is itself destroyed.
//
// Usage:
//      BumpPtrAllocator bumpPtrAlloc(4*1024*1024);
//      T* obj = bumpPtrAlloc.create<T>(arg1, arg2);
//
//      If you use the create method, obj will be deleted when 
//      bumpPtrAlloc goes out of scope.

class BumpPtrAllocatorSlab;
class BumpPtrAllocatorDestructor;

class BumpPtrAllocator {
public:
    using DestructFunc = std::function<void(void*)>;

    explicit BumpPtrAllocator(size_t defaultSlabSize);

    BumpPtrAllocator(const BumpPtrAllocator&) = delete;
    BumpPtrAllocator& operator=(const BumpPtrAllocator&) = delete;

    ~BumpPtrAllocator();

    template <typename T, typename... Args>
    T* create(Args&&... args) {
        T* obj = createWithNoDestruct<T>(std::forward<Args>(args)...);

        if constexpr (!std::is_trivially_destructible_v<T>) {
            registerDestructor(obj, [](void* ptr){ static_cast<T*>(ptr)->~T(); });
        }

        return obj;
    }

    // If you want to be absolutely sure that no destructor will be called
    template <typename T, typename... Args>
    T* createWithNoDestruct(Args&&... args) {
        return ::new (*this) T(std::forward<Args>(args)...);
    }

    void* allocate(size_t size, size_t align);

    void reset();

private:
    static constexpr size_t DEFAULT_SLAB_SIZE = 4*1024;

    const size_t _defaultSlabSize {DEFAULT_SLAB_SIZE};
    BumpPtrAllocatorSlab* _lastSlab {nullptr};
    BumpPtrAllocatorSlab* _freeSlab {nullptr};
    BumpPtrAllocatorDestructor* _lastDestruct {nullptr};

    void registerDestructor(void* ptr, const DestructFunc& dFunc);
};

// Placement new operator to use with the BumpPtrAllocator
// CAUTION: please use bumpPtrAlloc.create<T>() when possible
// because it calls the destructor for you.
//
// Usage:
//    T* obj = new (bumpPtrAlloc) (arg1, arg2);
// 
// and don't forget to call delete yourself for each object so
// that the destructor is called. 
//
// This must be done before the BumpPtrAllocator itself is deleted
// otherwise memory allocated possibly by members such as STL data structures
// will be lost.
//
// The allocator still handles memory freeing but the destructor still needs 
// to be called if you use explicit new.
//    delete (bumpPtrAlloc) obj;
//

inline void* operator new(size_t size,
                          std::align_val_t align,
                          BumpPtrAllocator& bumpPtrAlloc) {
    if (size == 0) {
        throw std::bad_alloc();
    }

    void* ptr = bumpPtrAlloc.allocate(size, static_cast<size_t>(align));
    if (!ptr) {
        throw std::bad_alloc();
    }
    return ptr;
}

inline void* operator new(size_t size,
                          BumpPtrAllocator& bumpPtrAlloc) {
    return operator new(size, std::align_val_t(alignof(::max_align_t)), bumpPtrAlloc);
}

inline void operator delete(void* ptr, BumpPtrAllocator& bumpPtrAlloc) noexcept {
    // Do nothing
}

inline void operator delete(void* ptr,
                            std::align_val_t align,
                            BumpPtrAllocator& bumpPtrAlloc) noexcept {
    // Do nothing
}
