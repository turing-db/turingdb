#include "GMLGraphParser.h"

using namespace db;

void GMLGraphSax::applyBufferedEdgeProperties() {
    for (const auto& prop : _bufferedEdgeProps) {
        std::visit([this, &prop](auto&& val) {
            using T = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<T, std::string>) {
                _currentPropName = prop.key;
                _currentPropName += " (String)";
                const auto propType = _metadata->getOrCreatePropertyType(
                    _currentPropName, ValueType::String);
                _builder->addEdgeProperty<types::String>(
                    *_currentEdge, propType._id, std::string(val));
            } else if constexpr (std::is_same_v<T, uint64_t>) {
                _currentPropName = prop.key;
                _currentPropName += " (UInt64)";
                const auto propType = _metadata->getOrCreatePropertyType(
                    _currentPropName, ValueType::UInt64);
                _builder->addEdgeProperty<types::UInt64>(
                    *_currentEdge, propType._id, val);
            } else if constexpr (std::is_same_v<T, int64_t>) {
                _currentPropName = prop.key;
                _currentPropName += " (Int64)";
                const auto propType = _metadata->getOrCreatePropertyType(
                    _currentPropName, ValueType::Int64);
                _builder->addEdgeProperty<types::Int64>(
                    *_currentEdge, propType._id, val);
            } else if constexpr (std::is_same_v<T, double>) {
                _currentPropName = prop.key;
                _currentPropName += " (Double)";
                const auto propType = _metadata->getOrCreatePropertyType(
                    _currentPropName, ValueType::Double);
                _builder->addEdgeProperty<types::Double>(
                    *_currentEdge, propType._id, val);
            }
        }, prop.value);
    }
    _bufferedEdgeProps.clear();
}
