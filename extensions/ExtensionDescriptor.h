#pragma once

#include <string>
#include <string_view>

namespace db {

class ExtensionDescriptor {
public:
    // Opaque handle to a dynamically loaded extension library
    using Handle = void*;

    ExtensionDescriptor(std::string_view name, Handle handle);
    ~ExtensionDescriptor();

    std::string_view getName() const { return _name; }
    Handle getHandle() const { return _handle; }

private:
    std::string _name;
    Handle _handle {nullptr};
};

}
