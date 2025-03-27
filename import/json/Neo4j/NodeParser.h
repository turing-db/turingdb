#pragma once

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include "GraphMetadata.h"
#include "writers/DataPartBuilder.h"
#include "IDMapper.h"
#include "labels/LabelMap.h"
#include "types/PropertyTypeMap.h"
#include "Parser.h"
#include "ControlCharacters.h"

namespace db::json::neo4j {

using json = nlohmann::json;

class NodeParser : public json::json_sax_t, public Parser {
public:
    NodeParser(GraphMetadata* graphMetadata,
               DataPartBuilder* buf,
               IDMapper* nodeIDMapper)
        : _propTypeMap(&graphMetadata->propTypes()),
          _labelMap(&graphMetadata->labels()),
          _buf(buf),
          _nodeIDMapper(nodeIDMapper)
    {
    }

    bool null() override {
        return true;
    }

    bool boolean(bool val) override {
        if (!_parsingNode) {
            return true;
        }

        if (_nesting == 7) {
            _currentPropName += " (Bool)";
            const PropertyType propType = _propTypeMap->get(_currentPropName);
            if (!propType.isValid()) {
                spdlog::info("Property type {} not supported", _currentPropName);
                return true;
            }

            _buf->addNodeProperty<types::Bool>(_currentNodeID, propType._id, val);
            return true;
        }

        return true;
    }

    bool number_integer(number_integer_t val) override {
        if (!_parsingNode) {
            return true;
        }

        if (_nesting == 7) {
            _currentPropName += " (Int64)";
            const PropertyType propType = _propTypeMap->get(_currentPropName);
            if (!propType.isValid()) {
                spdlog::info("Property type {} not supported", _currentPropName);
                return true;
            }

            _buf->addNodeProperty<types::Int64>(_currentNodeID, propType._id, val);
            return true;
        }

        return true;
    }

    bool number_unsigned(number_unsigned_t val) override {
        if (!_parsingNode) {
            return true;
        }


        if (_nesting == 7) {
            _currentPropName += " (UInt64)";
            const PropertyType propType = _propTypeMap->get(_currentPropName);
            if (!propType.isValid()) {
                spdlog::info("Property type {} not supported", _currentPropName);
                return true;
            }

            _buf->addNodeProperty<types::UInt64>(_currentNodeID, propType._id, val);
            return true;
        }

        if (_nesting == 6) {
            _currentNodeID = _buf->addNode(_labelset);
            _nodeIDMapper->registerID(val, _buf->getPartIndex(), _currentNodeID);
            return true;
        }

        return true;
    }

    bool number_float(number_float_t val, const string_t&) override {
        if (!_parsingNode) {
            return true;
        }

        if (_nesting == 7) {
            _currentPropName += " (Double)";
            const PropertyType propType = _propTypeMap->get(_currentPropName);
            if (!propType.isValid()) {
                spdlog::info("Property type {} not supported", _currentPropName);
                return true;
            }

            _buf->addNodeProperty<types::Double>(_currentNodeID, propType._id, val);
            return true;
        }

        return true;
    }

    bool string(string_t& val) override {
        if (!_parsingNode) {
            return true;
        }

        if (_parsingLabels) {
            const LabelID id = _labelMap->get(val);
            _labelset.set(id);
            return true;
        }

        if (_nesting == 7) {
            _currentPropName += " (String)";
            const PropertyType propType = _propTypeMap->get(_currentPropName);
            if (!propType.isValid()) {
                spdlog::info("Property type {} not supported", _currentPropName);
                return true;
            }

            std::string escaped;
            ControlCharactersEscaper::escape(val, escaped);

            _buf->addNodeProperty<types::String>(_currentNodeID, propType._id, std::move(escaped));
            return true;
        }

        return true;
    }

    bool start_object(std::size_t) override {
        _nesting++;
        if (_nesting == 5) {
            _labelset = LabelSet();
            return true;
        }

        return true;
    }

    bool end_object() override {
        _nesting--;
        if (_nesting == 4) {
            return true;
        }
        return true;
    }

    bool start_array(std::size_t) override {
        _nesting++;
        if (_nesting == 7) {
            _parsingLabels = true;
        }
        return true;
    }

    bool end_array() override {
        _nesting--;
        _parsingLabels = false;
        return true;
    }

    bool key(string_t& val) override {
        if (_nesting == 5) {
            if (val == "row") {
                _parsingNode = true;
                return true;
            } else {
                _parsingNode = false;
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
    PropertyTypeMap* _propTypeMap = nullptr;
    LabelMap* _labelMap = nullptr;
    DataPartBuilder* _buf;
    IDMapper* _nodeIDMapper;
    LabelSet _labelset;
    size_t _nesting = 0;
    EntityID _currentNodeID = 0;
    std::string _currentPropName;
    bool _parsingLabels = false;
    bool _parsingNode = false;
};

}
