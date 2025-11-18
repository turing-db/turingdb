#include "GetPropertiesProcessor.h"

using namespace db::v2;

template <typename PropertyChunkWriter>
std::string GetPropertiesProcessor<PropertyChunkWriter>::describe() const {
    return "";
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
