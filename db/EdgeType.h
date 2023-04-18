// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include "StringRef.h"

namespace db {

class ComponentType;
class Writeback;

class EdgeType {
public:
    friend Writeback;

    StringRef getName() const { return _name; }

    ComponentType* getSourceType() const { return _sourceType; }
    ComponentType* getTargetType() const { return _targetType; }

private:
    StringRef _name;
    ComponentType* _sourceType {nullptr};
    ComponentType* _targetType {nullptr};

    EdgeType(StringRef name,
             ComponentType* sourceType,
             ComponentType* targetType);
    ~EdgeType();
};

}
