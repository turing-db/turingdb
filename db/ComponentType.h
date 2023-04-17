#pragma once

#include "StringRef.h"

namespace db {

class Writeback;

class ComponentType {
public:
    friend Writeback;

    StringRef getName() const { return _name; }

private:
    StringRef _name;

    ComponentType(StringRef name);
    ~ComponentType();
};

}
