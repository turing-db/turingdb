#include "GetPropertiesProcessor.h"

#include <spdlog/fmt/fmt.h>

using namespace db::v2;

template <typename PropertyChunkWriter>
std::string GetPropertiesProcessor<PropertyChunkWriter>::describe() const {
    static_assert(std::is_same_v<typename PropertyChunkWriter::IDType, NodeID>
            || std::is_same_v<typename PropertyChunkWriter::IDType, EdgeID>);

    std::string idTypeStr;
    if constexpr (std::is_same_v<typename PropertyChunkWriter::IDType, NodeID>) {
        idTypeStr = "Node";
    } else {
        idTypeStr = "Edge";
    }

    constexpr auto valueType = PropertyChunkWriter::PropertyType::_valueType;
    constexpr std::string_view propValueType = ValueTypeName::value(valueType);

    return fmt::format("Get{}Properties{}Processor @=",
                       propValueType,
                       idTypeStr,
                       fmt::ptr(this));
}

template
std::string GetNodePropertiesInt64Processor::describe() const;
template
std::string GetNodePropertiesUInt64Processor::describe() const;
template
std::string GetNodePropertiesDoubleProcessor::describe() const;
template
std::string GetNodePropertiesStringProcessor::describe() const;
template
std::string GetNodePropertiesBoolProcessor::describe() const;

template
std::string GetEdgePropertiesInt64Processor::describe() const;
template
std::string GetEdgePropertiesUInt64Processor::describe() const;
template
std::string GetEdgePropertiesDoubleProcessor::describe() const;
template
std::string GetEdgePropertiesStringProcessor::describe() const;
template
std::string GetEdgePropertiesBoolProcessor::describe() const;
