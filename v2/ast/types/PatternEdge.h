#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "types/PatternEntity.h"

namespace db {

class MapLiteral;

class EdgePattern : public PatternEntity {
public:
    using EdgeTypeVector = std::vector<std::string_view>;

    EdgePattern() = default;
    ~EdgePattern() override = default;

    EdgePattern(const EdgePattern&) = default;
    EdgePattern(EdgePattern&&) = default;
    EdgePattern& operator=(const EdgePattern&) = default;
    EdgePattern& operator=(EdgePattern&&) = default;

    static std::unique_ptr<EdgePattern> create() {
        return std::make_unique<EdgePattern>();
    }

    const std::vector<std::string_view>& types() const {
        return _types.value();
    }

    std::vector<std::string_view>& types() {
        return _types.value();
    }

    bool hasTypes() const {
        return _types.has_value();
    }

    void setTypes(std::optional<EdgeTypeVector>&& types) {
        _types = std::move(types);
    }

private:
    std::optional<EdgeTypeVector> _types;
};

}
