#include "ExtensionDescriptor.h"

using namespace db;

ExtensionDescriptor::ExtensionDescriptor(std::string_view name,
                                         Handle handle)
    : _name(name),
    _handle(handle)
{
}

ExtensionDescriptor::~ExtensionDescriptor() {
}
