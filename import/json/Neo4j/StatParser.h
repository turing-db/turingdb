#pragma once

#include <nlohmann/json.hpp>

namespace db::json::neo4j {

using json = nlohmann::json;

class StatParser : public json::json_sax_t {
public:
    bool null() override {
        return true;
    }

    bool boolean(bool) override {
        return true;
    }

    bool number_integer(number_integer_t) override {
        return true;
    }

    bool number_unsigned(number_unsigned_t val) override {
        if (!_nodeCountParsed) {
            _nodeCount = val;
            _nodeCountParsed = true;
            return true;
        }

        if (!_edgeCountParsed) {
            _edgeCount = val;
            _edgeCountParsed = true;
            return true;
        }

        // Error
        return false;
    }

    bool number_float(number_float_t, const string_t&) override {
        return true;
    }

    bool string(string_t&) override {
        return true;
    }

    bool start_object(std::size_t) override {
        return true;
    }

    bool end_object() override {
        return true;
    }

    bool start_array(std::size_t) override {
        return true;
    }

    bool end_array() override {
        return true;
    }

    bool key(string_t&) override {
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

    size_t getNodeCount() const { return _nodeCount; }
    size_t getEdgeCount() const { return _edgeCount; }

private:
    bool _nodeCountParsed = false;
    bool _edgeCountParsed = false;
    size_t _nodeCount = 0;
    size_t _edgeCount = 0;
};
}
