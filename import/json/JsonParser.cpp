#include "JsonParser.h"

#include <range/v3/view/enumerate.hpp>

#include "Graph.h"
#include "Profiler.h"
#include "writers/DataPartBuilder.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "IDMapper.h"
#include "Neo4j/EdgeParser.h"
#include "Neo4j/EdgePropertyParser.h"
#include "Neo4j/EdgeTypeParser.h"
#include "Neo4j/NodeLabelParser.h"
#include "Neo4j/NodeLabelSetParser.h"
#include "Neo4j/NodeParser.h"
#include "Neo4j/NodePropertyParser.h"
#include "Neo4j/StatParser.h"
#include "JobSystem.h"

namespace db {

JsonParser::JsonParser(Graph* graph)
    : _graph(graph),
    _change(graph->newChange()),
    _commitBuilder(_change->access().getTip()),
    _nodeIDMapper(new IDMapper)
{
}

JsonParser::~JsonParser() = default;

CommitResult<void> JsonParser::buildPending(JobSystem& jobsystem) {
    return _commitBuilder->buildAllPending(jobsystem);
}

GraphStats JsonParser::parseStats(const std::string& data) {
    Profile profile {"JsonParser::parseStats"};
    auto parser = json::neo4j::StatParser();
    nlohmann::json::sax_parse(data,
                              &parser,
                              nlohmann::json::input_format_t::json,
                              true,
                              true);
    _nodeIDMapper->reserve(parser.getNodeCount());
    return {parser.getNodeCount(), parser.getEdgeCount()};
}

bool JsonParser::parseNodeLabels(const std::string& data) {
    Profile profile {"JsonParser::parseNodeLabels"};
    auto parser = json::neo4j::NodeLabelParser(&_commitBuilder->metadata());
    return nlohmann::json::sax_parse(data,
                                     &parser,
                                     nlohmann::json::input_format_t::json,
                                     true,
                                     true);
}

bool JsonParser::parseNodeLabelSets(const std::string& data) {
    Profile profile {"JsonParser::parseNodeLabelSets"};
    auto parser = json::neo4j::NodeLabelSetParser(&_commitBuilder->metadata());
    return nlohmann::json::sax_parse(data,
                                     &parser,
                                     nlohmann::json::input_format_t::json,
                                     true,
                                     true);
}

bool JsonParser::parseEdgeTypes(const std::string& data) {
    Profile profile {"JsonParser::parseEdgeTypes"};
    auto parser = json::neo4j::EdgeTypeParser(&_commitBuilder->metadata());
    return nlohmann::json::sax_parse(data,
                                     &parser,
                                     nlohmann::json::input_format_t::json,
                                     true,
                                     true);
}

bool JsonParser::parseNodeProperties(const std::string& data) {
    Profile profile {"JsonParser::parseNodeProperties"};
    auto parser = json::neo4j::NodePropertyParser(&_commitBuilder->metadata());
    return nlohmann::json::sax_parse(data,
                                     &parser,
                                     nlohmann::json::input_format_t::json,
                                     true,
                                     true);
}

bool JsonParser::parseEdgeProperties(const std::string& data) {
    Profile profile {"JsonParser::parseEdgeProperties"};
    auto parser = json::neo4j::EdgePropertyParser(&_commitBuilder->metadata());
    return nlohmann::json::sax_parse(data,
                                     &parser,
                                     nlohmann::json::input_format_t::json,
                                     true,
                                     true);
}

bool JsonParser::parseNodes(const std::string& data, DataPartBuilder& buf) {
    Profile profile {"JsonParser::parseNodes"};
    auto parser = json::neo4j::NodeParser(&_commitBuilder->metadata(), &buf, _nodeIDMapper.get());
    return nlohmann::json::sax_parse(data, &parser,
                                     nlohmann::json::input_format_t::json,
                                     true, true);
}

bool JsonParser::parseEdges(const std::string& data, DataPartBuilder& buf) {
    Profile profile {"JsonParser::parseEdges"};
    auto parser = json::neo4j::EdgeParser(&_commitBuilder->metadata(),
                                          &buf,
                                          _nodeIDMapper.get(),
                                          _commitBuilder->readGraph());
    return nlohmann::json::sax_parse(data, &parser,
                                     nlohmann::json::input_format_t::json,
                                     true, true);
}

DataPartBuilder& JsonParser::getCurrentDataBuffer() {
    return _commitBuilder->getCurrentBuilder();
}

DataPartBuilder& JsonParser::newDataBuffer() {
    return _commitBuilder->newBuilder();
}

CommitResult<void> JsonParser::commit(Graph& graph, JobSystem& jobSystem) {
    return _change->access().submit(jobSystem);
}

}
