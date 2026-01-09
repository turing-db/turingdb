#include "JsonParser.h"

#include <range/v3/view/enumerate.hpp>

#include "Graph.h"
#include "Profiler.h"
#include "writers/DataPartBuilder.h"
#include "versioning/ChangeAccessor.h"
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

JsonParser::JsonParser()
    : _nodeIDMapper(new IDMapper)
{
}

JsonParser::~JsonParser() = default;

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

bool JsonParser::parseNodeLabels(ChangeAccessor& change, const std::string& data) {
    Profile profile {"JsonParser::parseNodeLabels"};
    CommitBuilder* tip = change.getTip();
    auto parser = json::neo4j::NodeLabelParser(&tip->metadata());
    return nlohmann::json::sax_parse(data,
                                     &parser,
                                     nlohmann::json::input_format_t::json,
                                     true,
                                     true);
}

bool JsonParser::parseNodeLabelSets(ChangeAccessor& change, const std::string& data) {
    Profile profile {"JsonParser::parseNodeLabelSets"};
    CommitBuilder* tip = change.getTip();
    auto parser = json::neo4j::NodeLabelSetParser(&tip->metadata());
    return nlohmann::json::sax_parse(data,
                                     &parser,
                                     nlohmann::json::input_format_t::json,
                                     true,
                                     true);
}

bool JsonParser::parseEdgeTypes(ChangeAccessor& change, const std::string& data) {
    Profile profile {"JsonParser::parseEdgeTypes"};
    CommitBuilder* tip = change.getTip();
    auto parser = json::neo4j::EdgeTypeParser(&tip->metadata());
    return nlohmann::json::sax_parse(data,
                                     &parser,
                                     nlohmann::json::input_format_t::json,
                                     true,
                                     true);
}

bool JsonParser::parseNodeProperties(ChangeAccessor& change, const std::string& data) {
    Profile profile {"JsonParser::parseNodeProperties"};
    CommitBuilder* tip = change.getTip();
    auto parser = json::neo4j::NodePropertyParser(&tip->metadata());
    return nlohmann::json::sax_parse(data,
                                     &parser,
                                     nlohmann::json::input_format_t::json,
                                     true,
                                     true);
}

bool JsonParser::parseEdgeProperties(ChangeAccessor& change, const std::string& data) {
    Profile profile {"JsonParser::parseEdgeProperties"};
    CommitBuilder* tip = change.getTip();
    auto parser = json::neo4j::EdgePropertyParser(&tip->metadata());
    return nlohmann::json::sax_parse(data,
                                     &parser,
                                     nlohmann::json::input_format_t::json,
                                     true,
                                     true);
}

bool JsonParser::parseNodes(ChangeAccessor& change, const std::string& data) {
    Profile profile {"JsonParser::parseNodes"};
    CommitBuilder* tip = change.getTip();
    auto parser = json::neo4j::NodeParser(&tip->metadata(),
                                          &tip->getCurrentBuilder(),
                                          _nodeIDMapper.get());
    return nlohmann::json::sax_parse(data, &parser,
                                     nlohmann::json::input_format_t::json,
                                     true, true);
}

bool JsonParser::parseEdges(ChangeAccessor& change, const std::string& data) {
    Profile profile {"JsonParser::parseEdges"};
    CommitBuilder* tip = change.getTip();
    auto parser = json::neo4j::EdgeParser(&tip->metadata(),
                                          &tip->getCurrentBuilder(),
                                          _nodeIDMapper.get(),
                                          tip->readGraph());
    return nlohmann::json::sax_parse(data, &parser,
                                     nlohmann::json::input_format_t::json,
                                     true, true);
}

}
