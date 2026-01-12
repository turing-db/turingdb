#pragma once

#include <nlohmann/json.hpp>
#include <regex>
#include <spdlog/spdlog.h>

#include "Graph.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "writers/DataPartBuilder.h"
#include "IDMapper.h"
#include "Parser.h"
#include "ControlCharacters.h"
#include "writers/MetadataBuilder.h"

namespace db::json::neo4j {

using json = nlohmann::json;

class EdgeParser : public json::json_sax_t, public Parser {
public:
    EdgeParser(MetadataBuilder* metadata,
               DataPartBuilder* buf,
               IDMapper* nodeIDMapper,
               const GraphReader& reader)
        : _buf(buf),
        _metadata(metadata),
        _nodeIDMapper(nodeIDMapper),
        _reader(reader)
    {
    }

    bool null() override {
        return true;
    }

    bool boolean(bool val) override {
        if (!_parsingEdge) {
            return true;
        }

        if (_nesting == 7) {
            _currentPropName += " (Bool)";
            const PropertyType propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::Bool);
            if (!propType.isValid()) {
                spdlog::info("Property type {} not supported", _currentPropName);
                return true;
            }

            _buf->addEdgeProperty<types::Bool>(*_currentEdge, propType._id, val);
            return true;
        }

        return true;
    }

    bool number_integer(number_integer_t val) override {
        if (!_parsingEdge) {
            return true;
        }

        if (_nesting == 7) {
            _currentPropName += " (Int64)";
            const PropertyType propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::Int64);
            if (!propType.isValid()) {
                spdlog::info("Property type {} not supported", _currentPropName);
                return true;
            }

            _buf->addEdgeProperty<types::Int64>(*_currentEdge, propType._id, val);
            return true;
        }

        return true;
    }

    bool number_unsigned(number_unsigned_t val) override {
        if (!_parsingEdge) {
            return true;
        }

        if (_nesting == 7) {
            _currentPropName += " (UInt64)";
            const PropertyType propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::UInt64);
            if (!propType.isValid()) {
                spdlog::info("Property type {} not supported", _currentPropName);
                return true;
            }

            _buf->addEdgeProperty<types::UInt64>(*_currentEdge, propType._id, val);
            return true;
        }

        if (_nesting == 6) {
            if (!_parsedSourceID) {
                _sourceID = val;
                _parsedSourceID = true;
                return true;
            }

            if (!_parsedTargetID) {
                _targetID = val;
                _parsedTargetID = true;
                const NodeID srcID = _nodeIDMapper->getID(_sourceID);
                const NodeID tgtID = _nodeIDMapper->getID(_targetID);

                _currentEdge = &_buf->addEdge(_edgeTypeID, srcID, tgtID);
                return true;
            }

            // Error
            return false;
        }
        return true;
    }

    bool number_float(number_float_t val, const string_t&) override {
        if (!_parsingEdge) {
            return true;
        }

        if (_nesting == 7) {
            _currentPropName += " (Double)";
            const PropertyType propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::Double);
            if (!propType.isValid()) {
                spdlog::info("Property type {} not supported", _currentPropName);
                return true;
            }

            _buf->addEdgeProperty<types::Double>(*_currentEdge, propType._id, val);
            return true;
        }

        return true;
    }

    bool string(string_t& val) override {
        if (!_parsingEdge) {
            return true;
        }

        if (_nesting == 7) {
            _currentPropName += " (String)";
            const PropertyType propType = _metadata->getOrCreatePropertyType(_currentPropName, ValueType::String);
            if (!propType.isValid()) {
                spdlog::info("Property type {} not supported", _currentPropName);
                return true;
            }

            std::string escaped;
            ControlCharactersEscaper::escape(val, escaped);
            _buf->addEdgeProperty<types::String>(*_currentEdge, propType._id, std::move(escaped));
            return true;
        }

        if (_nesting == 6) {
            if (!_parsedEdgeType) {
                _parsedEdgeType = true;
                _edgeTypeID = _metadata->getOrCreateEdgeType(val);
                return true;
            }
        }
        return true;
    }

    bool start_object(std::size_t) override {
        _nesting++;
        _parsedEdgeType = false;
        _parsedSourceID = false;
        _parsedTargetID = false;
        return true;
    }

    bool end_object() override {
        _nesting--;
        return true;
    }

    bool start_array(std::size_t) override {
        _nesting++;
        return true;
    }

    bool end_array() override {
        _nesting--;
        return true;
    }

    bool key(string_t& val) override {
        if (_nesting == 5) {
            if (val == "row") {
                _parsingEdge = true;
                return true;
            } else {
                _parsingEdge = false;
                return true;
            }
        }

        if (_nesting == 7) {
            _currentPropName = val;
            return true;
        }

        return true;
    }

    bool binary(json::binary_t&) override {
        return true;
    }

    bool parse_error(std::size_t,
                     const std::string&,
                     const json::exception&) override {
        return false;
    }

private:
    DataPartBuilder* _buf {nullptr};
    MetadataBuilder* _metadata {nullptr};
    const IDMapper* _nodeIDMapper;
    GraphReader _reader;
    size_t _nesting = 0;
    const EdgeRecord* _currentEdge {nullptr};
    bool _parsingEdge = false;
    bool _parsedEdgeType = false;
    bool _parsedSourceID = false;
    bool _parsedTargetID = false;
    std::string _currentPropName;
    EdgeTypeID _edgeTypeID = 0;
    uint64_t _sourceID = 0;
    uint64_t _targetID = 0;
    std::regex _regEscapes {"\"|\\\\"};
    std::regex _regNewline {"\n"};
};
}
