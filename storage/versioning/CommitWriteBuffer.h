#pragma once

#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "ID.h"
#include "writers/DataPartBuilder.h"
#include "metadata/PropertyType.h"

namespace {

using namespace db;
struct AddNodeProperty {
    AddNodeProperty(DataPartBuilder& builder, NodeID nid, PropertyTypeID pid)
        : _builder(builder),
          _nid(nid),
          _pid(pid) {}

    void operator()(types::Int64::Primitive propValue) const {
        _builder.addNodeProperty<types::Int64>(_nid, _pid, propValue);
    }
    void operator()(types::UInt64::Primitive propValue) const {
        _builder.addNodeProperty<types::UInt64>(_nid, _pid, propValue);
    }
    void operator()(types::Double::Primitive propValue) const {
        _builder.addNodeProperty<types::Double>(_nid, _pid, propValue);
    }
    void operator()(types::String::Primitive propValue) const {
        _builder.addNodeProperty<types::String>(_nid, _pid, propValue);
    }
    void operator()(types::Bool::Primitive propValue) const {
        _builder.addNodeProperty<types::Bool>(_nid, _pid, propValue);
    }


private:
    DataPartBuilder& _builder;
    NodeID _nid;
    PropertyTypeID _pid;
};
} // Anonymous namespace

namespace db {

class CommitWriteBuffer {
public:
    using TypeVariant = std::variant<types::Int64::Primitive, types::UInt64::Primitive,
                                     types::Double::Primitive, types::String::Primitive,
                                     types::Bool::Primitive>;
private:
    struct UntypedProperty {
        std::string propertyName;
        TypeVariant value;
    };

    struct PendingNode{
        std::vector<std::string> labelNames;
        std::vector<UntypedProperty> properties;
    };

public:

    void createNode(std::vector<std::string> labels, std::vector<UntypedProperty> properties) {
        _pendingNodes.emplace_back(labels, properties);
    }

    void func() {
        TypeVariant x {std::in_place_type<types::Bool::Primitive>, false};
        std::visit(AddNodeProperty {_builder, 0, 0}, x);
    }
    

    /*
    void func() {


        std::visit(
            [this](const auto& propertyValue) {
                using T = std::decay_t<decltype(propertyValue)>;
                if constexpr (std::is_same_v<T, types::Int64::Primitive>) {
                    _builder.addNodeProperty<types::Int64>(0, 0, propertyValue);
                } else if constexpr (std::is_same_v<T, types::UInt64::Primitive>) {
                    _builder.addNodeProperty<types::UInt64>(0, 0, propertyValue);
                } else if constexpr (std::is_same_v<T, types::Double::Primitive>) {
                    _builder.addNodeProperty<types::Double>(0, 0, propertyValue);
                } else if constexpr (std::is_same_v<T, types::String::Primitive>) {
                    _builder.addNodeProperty<types::String>(0, 0, propertyValue);
                } else if constexpr (std::is_same_v<T, types::Bool::Primitive>) {
                    _builder.addNodeProperty<types::Bool>(0, 0, propertyValue);
                } else {
                    throw TuringException("Property had unknown type");
                }
            },
            x);
    }
    */

private:
    DataPartBuilder _builder;
    std::vector<PendingNode> _pendingNodes;
};
    
}
